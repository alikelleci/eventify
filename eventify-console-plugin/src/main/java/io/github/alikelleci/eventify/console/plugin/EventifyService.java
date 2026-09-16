package io.github.alikelleci.eventify.console.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.protocol.InstanceStatus;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.alikelleci.eventify.core.messaging.Metadata.REPLY_TO;

@Slf4j
public class EventifyService {

  private static final String EVENT_STORE = "event-store";
  private static final String SNAPSHOT_STORE = "snapshot-store";

  public record CommandsPage(List<Command> commands) {}
  public record EventsPage(List<Event> events, String nextCursor) {}
  public record EventDetail(Event event, AggregateState state, AggregateState previousState) {}
  public record CorrelatedEventsPage(List<Event> events) {}

  public sealed interface ApiResult<T> {
    record Ok<T>(T value) implements ApiResult<T> {}
    record NotFound<T>() implements ApiResult<T> {}
    /** Another instance owns the aggregate; {@code owner} is its {@code application.server}. */
    record NotOwner<T>(String owner) implements ApiResult<T> {}
    record Unavailable<T>(String reason) implements ApiResult<T> {}
  }

  private final Eventify eventify;
  private final StatusTracker statusTracker;
  private final HostInfo thisHost;
  private final ObjectMapper objectMapper;
  private final Producer<String, Command> producer;


  public EventifyService(Eventify eventify, StatusTracker statusTracker) {
    this.eventify = eventify;
    this.statusTracker = statusTracker;
    this.objectMapper = eventify.getObjectMapper();
    this.thisHost = hostInfo(eventify);

    if (thisHost.equals(HostInfo.unavailable())) {
      log.warn("'{}' is not configured, running in single-node mode. Multi-node routing is disabled.",
          StreamsConfig.APPLICATION_SERVER_CONFIG);
    }

    String bootstrapServers = eventify.getStreamsConfig().getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    this.producer = new KafkaProducer<>(producerProps, new StringSerializer(), new JsonSerializer<>(objectMapper));
  }

  /**
   * How this instance is doing: its Kafka Streams state, how long it has been in it, and what it is restoring.
   * Everything is read from what Kafka Streams already keeps in memory: no calls to Kafka, and not on the stream threads.
   */
  public ApiResult<InstanceStatus> getStatus() {
    KafkaStreams streams = eventify.getKafkaStreams();
    if (streams == null) {
      return new ApiResult.Unavailable<>("Eventify is not started");
    }

    return new ApiResult.Ok<>(new InstanceStatus(streams.state().name(), statusTracker.stateForMs(), statusTracker.restore()));
  }

  public void close() {
    producer.close();
  }

  /**
   * This instance's {@code application.server}, or {@link HostInfo#unavailable()} if it isn't set. Eventify always sets
   * it; the fallback keeps the service safe when it is used without it.
   */
  static HostInfo hostInfo(Eventify eventify) {
    String applicationServer = eventify.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, "");
    HostInfo hostInfo = HostInfo.buildFromEndpoint(applicationServer);
    return hostInfo != null ? hostInfo : HostInfo.unavailable();
  }

  /** How instances refer to each other: {@link #hostInfo(Eventify)} as {@code host:port}. */
  static String nodeId(HostInfo hostInfo) {
    return hostInfo.host() + ":" + hostInfo.port();
  }

  public ApiResult<Void> retryCommand(Command original) {
    Metadata retryMetadata = Metadata.builder()
        .putAll(original.getMetadata())
        .put("retry", "true")
        .put("source", "console")
        .put("description", "Retried via Eventify Console")
        .build();
    retryMetadata.remove(REPLY_TO);

    Command retryCommand = Command.builder()
        .payload(original.getPayload())
        .metadata(retryMetadata)
        .build();

    String commandTopic = original.getTopicInfo().value();

    try {
      producer.send(new ProducerRecord<>(commandTopic, null, retryCommand.getTimestamp().toEpochMilli(),
          retryCommand.getAggregateId(), retryCommand));
      log.info("Retried command {} as {} on topic {}", original.getId(), retryCommand.getId(), commandTopic);
    } catch (Exception e) {
      log.error("Failed to publish retry command for {}", original.getId(), e);
      return new ApiResult.Unavailable<>("Failed to publish retry command");
    }

    return new ApiResult.Ok<>(null);
  }

  /**
   * Reads the aggregate's commands from the result topics. Every call has its own consumer, so calls never affect each
   * other. When the request is cancelled, only this call's consumer stops, the way Kafka intends: with a wakeup.
   */
  public ApiResult<CommandsPage> getCommands(String aggregateId, int limit, CancelSignal cancel) {
    // Eventify writes the result of every handled command to its command topic with .results.
    Set<String> resultTopics = eventify.getCommandTopics().stream()
        .map(topic -> topic.concat(".results"))
        .collect(Collectors.toSet());
    if (resultTopics.isEmpty()) {
      return new ApiResult.Ok<>(new CommandsPage(List.of()));
    }

    String bootstrapServers = eventify.getStreamsConfig().getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

    List<Command> results = new ArrayList<>();
    JsonDeserializer<Command> commandDeserializer = new JsonDeserializer<>(Command.class, objectMapper);

    // Closed in reverse order: the wakeup is unregistered before the consumer closes.
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
         AutoCloseable stopOnCancel = cancel.onCancel(consumer::wakeup)) {
      for (String topic : resultTopics) {
        try {
          List<TopicPartition> allPartitions = consumer.partitionsFor(topic).stream()
              .map(pi -> new TopicPartition(topic, pi.partition()))
              .toList();

          if (allPartitions.isEmpty()) continue;

          int numPartitions = allPartitions.size();
          int partition = Utils.toPositive(Utils.murmur2(aggregateId.getBytes(java.nio.charset.StandardCharsets.UTF_8))) % numPartitions;
          TopicPartition tp = new TopicPartition(topic, partition);

          consumer.assign(Collections.singletonList(tp));

          long lookbackMs = Instant.now().minus(7, ChronoUnit.DAYS).toEpochMilli();
          OffsetAndTimestamp offsetAndTimestamp = consumer.offsetsForTimes(Map.of(tp, lookbackMs)).get(tp);
          long startOffset = offsetAndTimestamp != null ? offsetAndTimestamp.offset() : 0L;
          consumer.seek(tp, startOffset);

          Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(tp));
          long endOffset = endOffsets.getOrDefault(tp, 0L);
          if (startOffset >= endOffset) continue;

          boolean done = false;
          while (!done) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            if (records.isEmpty()) break;
            for (ConsumerRecord<String, String> record : records) {
              if (record.offset() >= endOffset) { done = true; break; }
              if (!aggregateId.equals(record.key())) continue;
              if (record.value() == null) continue;
              try {
                Command command = commandDeserializer.deserialize(topic, record.value().getBytes());
                if (command != null) results.add(command);
              } catch (Exception e) {
                log.warn("Failed to deserialize command record on topic {}", topic, e);
              }
            }
          }
        } catch (WakeupException e) {
          throw e;
        } catch (Exception e) {
          log.warn("Failed to query command result topic {}", topic, e);
        }
      }
    } catch (WakeupException e) {
      log.debug("Stopped reading commands for aggregate {}: the request was cancelled", aggregateId);
      return new ApiResult.Unavailable<>("Cancelled");
    } catch (Exception e) {
      log.error("Unexpected error querying commands for aggregate {}", aggregateId, e);
      return new ApiResult.Unavailable<>("Unexpected error");
    }

    results.sort((a, b) -> b.getTimestamp().compareTo(a.getTimestamp()));
    List<Command> limited = results.size() > limit ? results.subList(0, limit) : results;
    return new ApiResult.Ok<>(new CommandsPage(limited));
  }

  public ApiResult<CorrelatedEventsPage> getEventsByCorrelation(String aggregateId, String correlationId) {
    ApiResult<CorrelatedEventsPage> routing = checkRouting(aggregateId);
    if (routing != null) return routing;

    try {
      ReadOnlyKeyValueStore<String, Event> store = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));

      List<Event> events = new ArrayList<>();
      try (KeyValueIterator<String, Event> it = store.range(aggregateId + "@", aggregateId + "@~")) {
        while (it.hasNext()) {
          Event event = it.next().value;
          if (correlationId.equals(event.getMetadata().get("$correlationId"))) {
            events.add(event);
          }
        }
      }
      return new ApiResult.Ok<>(new CorrelatedEventsPage(events));
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return new ApiResult.Unavailable<>("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying correlated events for aggregate {}", aggregateId, e);
      return new ApiResult.Unavailable<>("Unexpected error");
    }
  }

  public ApiResult<EventsPage> getEvents(String aggregateId, String cursor, int limit) {
    ApiResult<EventsPage> routing = checkRouting(aggregateId);
    if (routing != null) {
      return routing;
    }

    try {
      ReadOnlyKeyValueStore<String, Event> store = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));

      String from = aggregateId + "@";
      String to = cursor != null ? aggregateId + "@" + cursor + "\0" : aggregateId + "@~";

      List<Event> events = new ArrayList<>();
      try (KeyValueIterator<String, Event> it = store.reverseRange(from, to)) {
        while (it.hasNext() && events.size() <= limit) {
          events.add(it.next().value);
        }
      }

      String nextCursor = null;
      if (events.size() > limit) {
        Event extra = events.remove(events.size() - 1);
        nextCursor = extra.getId().substring(aggregateId.length() + 1);
      }

      return new ApiResult.Ok<>(new EventsPage(events, nextCursor));
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return new ApiResult.Unavailable<>("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying events for aggregate {}", aggregateId, e);
      return new ApiResult.Unavailable<>("Unexpected error");
    }
  }

  public ApiResult<EventDetail> getEventDetail(String aggregateId, String eventId) {
    ApiResult<EventDetail> routing = checkRouting(aggregateId);
    if (routing != null) {
      return routing;
    }

    try {
      ReadOnlyKeyValueStore<String, AggregateState> snapshotStore = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(SNAPSHOT_STORE, QueryableStoreTypes.keyValueStore()));
      ReadOnlyKeyValueStore<String, Event> eventStore = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));

      Event targetEvent = eventStore.get(eventId);
      if (targetEvent == null) {
        if (!isLocallyAuthoritative(aggregateId)) {
          log.debug("Ownership/availability changed while querying aggregate {}; returning 503", aggregateId);
          return new ApiResult.Unavailable<>("Ownership changed during query");
        }
        return new ApiResult.NotFound<>();
      }

      String from = aggregateId + "@";
      String to = eventId;

      AggregateState state = Optional.ofNullable(snapshotStore.get(aggregateId))
          .filter(snap -> snap.getEventId().compareTo(to) < 0)
          .orElse(null);

      if (state != null) {
        from = state.getEventId() + "\0";
      }

      long version = state != null ? state.getVersion() : 0;

      AggregateState previousState = state;
      long previousVersion = version;

      try (KeyValueIterator<String, Event> it = eventStore.range(from, to)) {
        while (it.hasNext()) {
          Event event = it.next().value;
          if (event.getId().equals(eventId)) {
            previousState = state;
            previousVersion = version;
          }
          EventSourcingHandler handler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
          if (handler != null) {
            state = handler.apply(state, event);
            version++;
          }
        }
      }

      AggregateState currentState = state == null ? null : AggregateState.builder()
          .timestamp(state.getTimestamp())
          .payload(state.getPayload())
          .metadata(state.getMetadata())
          .eventId(state.getEventId())
          .version(version)
          .build();

      AggregateState previousStateResult = previousState == null ? null : AggregateState.builder()
          .timestamp(previousState.getTimestamp())
          .payload(previousState.getPayload())
          .metadata(previousState.getMetadata())
          .eventId(previousState.getEventId())
          .version(previousVersion)
          .build();

      return new ApiResult.Ok<>(new EventDetail(targetEvent, currentState, previousStateResult));
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return new ApiResult.Unavailable<>("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying event detail for aggregate {}", aggregateId, e);
      return new ApiResult.Unavailable<>("Unexpected error");
    }
  }

  public ApiResult<AggregateState> getState(String aggregateId, String eventId) {
    ApiResult<AggregateState> routing = checkRouting(aggregateId);
    if (routing != null) {
      return routing;
    }

    try {
      ReadOnlyKeyValueStore<String, AggregateState> snapshotStore = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(SNAPSHOT_STORE, QueryableStoreTypes.keyValueStore()));
      ReadOnlyKeyValueStore<String, Event> eventStore = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));

      String from = aggregateId + "@";
      String to = eventId != null ? eventId : aggregateId + "@~";

      AggregateState state = Optional.ofNullable(snapshotStore.get(aggregateId))
          .filter(snap -> eventId == null || snap.getEventId().compareTo(to) <= 0)
          .orElse(null);

      if (state != null) {
        from = state.getEventId() + "\0";
      }

      long version = state != null ? state.getVersion() : 0;

      try (KeyValueIterator<String, Event> it = eventStore.range(from, to)) {
        while (it.hasNext()) {
          Event event = it.next().value;
          EventSourcingHandler handler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
          if (handler != null) {
            state = handler.apply(state, event);
            version++;
          }
        }
      }

      if (state == null) {
        if (!isLocallyAuthoritative(aggregateId)) {
          log.debug("Ownership/availability changed while querying aggregate {}; returning 503", aggregateId);
          return new ApiResult.Unavailable<>("Ownership changed during query");
        }
        return new ApiResult.NotFound<>();
      }

      state = AggregateState.builder()
          .timestamp(state.getTimestamp())
          .payload(state.getPayload())
          .metadata(state.getMetadata())
          .eventId(state.getEventId())
          .version(version)
          .build();

      return new ApiResult.Ok<>(state);
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return new ApiResult.Unavailable<>("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying state for aggregate {}", aggregateId, e);
      return new ApiResult.Unavailable<>("Unexpected error");
    }
  }

  /**
   * Returns {@code null} when this instance can answer for the aggregate, or the reason it can't: Kafka Streams isn't
   * running, or another instance owns the aggregate. The console then asks that instance instead.
   */
  private <T> ApiResult<T> checkRouting(String aggregateId) {
    KafkaStreams streams = eventify.getKafkaStreams();

    if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
      log.debug("Kafka Streams is not running");
      return new ApiResult.Unavailable<>("Kafka Streams is not running");
    }

    if (thisHost.equals(HostInfo.unavailable())) {
      return null;
    }

    KeyQueryMetadata metadata = streams.queryMetadataForKey(EVENT_STORE, aggregateId, Serdes.String().serializer());
    if (metadata == null || metadata.activeHost().equals(HostInfo.unavailable())) {
      log.warn("Metadata unavailable for aggregate {}", aggregateId);
      return new ApiResult.Unavailable<>("Metadata unavailable");
    }

    HostInfo activeHost = metadata.activeHost();
    if (activeHost.equals(thisHost)) {
      return null;
    }

    return new ApiResult.NotOwner<>(nodeId(activeHost));
  }

  private boolean isLocallyAuthoritative(String aggregateId) {
    KafkaStreams streams = eventify.getKafkaStreams();
    if (streams.state() != KafkaStreams.State.RUNNING) {
      return false;
    }
    if (thisHost.equals(HostInfo.unavailable())) {
      return true;
    }
    KeyQueryMetadata metadata = streams.queryMetadataForKey(EVENT_STORE, aggregateId, Serdes.String().serializer());
    return metadata != null && thisHost.equals(metadata.activeHost());
  }
}
