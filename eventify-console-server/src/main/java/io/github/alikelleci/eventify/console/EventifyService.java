package io.github.alikelleci.eventify.console;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
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

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

@Slf4j
public class EventifyService {

  private static final String EVENT_STORE = "event-store";
  private static final String SNAPSHOT_STORE = "snapshot-store";

  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);

  public record CommandsPage(List<Command> commands) {}
  public record EventsPage(List<Event> events, String nextCursor) {}
  public record EventDetail(Event event, AggregateState state, AggregateState previousState) {}
  public record CorrelatedEventsPage(List<Event> events) {}

  public sealed interface ApiResult<T> {
    record Ok<T>(T value) implements ApiResult<T> {}
    record NotFound<T>() implements ApiResult<T> {}
    record Unavailable<T>(String reason) implements ApiResult<T> {}
    record RemoteError<T>(int statusCode) implements ApiResult<T> {}
  }

  private final Eventify eventify;
  private final HostInfo thisHost;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final Producer<String, Command> producer;

  public EventifyService(Eventify eventify) {
    this.eventify = eventify;
    this.objectMapper = eventify.getObjectMapper();
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(CONNECT_TIMEOUT)
        .build();

    String applicationServer = eventify.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, "");
    URI uri = applicationServer.isBlank() ? null : URI.create("http://" + applicationServer);
    this.thisHost = (uri != null && uri.getHost() != null && uri.getPort() != -1)
        ? new HostInfo(uri.getHost(), uri.getPort())
        : HostInfo.unavailable();

    if (thisHost.equals(HostInfo.unavailable())) {
      log.warn("'{}' is not configured, running in single-node mode. Multi-node routing is disabled.",
          StreamsConfig.APPLICATION_SERVER_CONFIG);
    }

    String bootstrapServers = eventify.getStreamsConfig().getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    this.producer = new KafkaProducer<>(producerProps, new StringSerializer(), new JsonSerializer<>(objectMapper));
  }

  public void close() {
    producer.close();
  }

  public ApiResult<Void> retryCommand(Command original) {
    Metadata retryMetadata = Metadata.builder()
        .putAll(original.getMetadata())
        .build();
    retryMetadata.remove(Metadata.REPLY_TO);

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

  public ApiResult<CommandsPage> getCommands(String aggregateId, int limit) {
    Set<String> resultTopics = eventify.getResultTopics();
    if (resultTopics.isEmpty()) {
      return new ApiResult.Ok<>(new CommandsPage(List.of()));
    }

    String bootstrapServers = eventify.getStreamsConfig().getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

    List<Command> results = new ArrayList<>();
    JsonDeserializer<Command> commandDeserializer = new JsonDeserializer<>(Command.class, objectMapper);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
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
        } catch (Exception e) {
          log.warn("Failed to query command result topic {}", topic, e);
        }
      }
    } catch (Exception e) {
      log.error("Unexpected error querying commands for aggregate {}", aggregateId, e);
      return new ApiResult.Unavailable<>("Unexpected error");
    }

    results.sort((a, b) -> b.getTimestamp().compareTo(a.getTimestamp()));
    List<Command> limited = results.size() > limit ? results.subList(0, limit) : results;
    return new ApiResult.Ok<>(new CommandsPage(limited));
  }

  public ApiResult<CorrelatedEventsPage> getEventsByCorrelation(String aggregateId, String correlationId, boolean forwarded) {
    ApiResult<CorrelatedEventsPage> routing = checkRouting(aggregateId, forwarded,
        "/api/aggregates/" + URLEncoder.encode(aggregateId, java.nio.charset.StandardCharsets.UTF_8)
            + "/events/by-correlation/" + URLEncoder.encode(correlationId, java.nio.charset.StandardCharsets.UTF_8),
        new TypeReference<CorrelatedEventsPage>() {});
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

  public ApiResult<EventsPage> getEvents(String aggregateId, String cursor, int limit, boolean forwarded) {
    ApiResult<EventsPage> routing = checkRouting(aggregateId, forwarded,
        "/api/aggregates/" + URLEncoder.encode(aggregateId, java.nio.charset.StandardCharsets.UTF_8) + "/events" + buildEventsQuery(cursor, limit),
        new TypeReference<EventsPage>() {});
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

  public ApiResult<EventDetail> getEventDetail(String aggregateId, String eventId, boolean forwarded) {
    ApiResult<EventDetail> routing = checkRouting(aggregateId, forwarded,
        "/api/aggregates/" + URLEncoder.encode(aggregateId, java.nio.charset.StandardCharsets.UTF_8) + "/events/" + URLEncoder.encode(eventId, java.nio.charset.StandardCharsets.UTF_8),
        new TypeReference<EventDetail>() {});
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

  public ApiResult<AggregateState> getState(String aggregateId, String eventId, boolean forwarded) {
    ApiResult<AggregateState> routing = checkRouting(aggregateId, forwarded,
        "/api/aggregates/" + URLEncoder.encode(aggregateId, java.nio.charset.StandardCharsets.UTF_8) + "/state" + buildStateQuery(eventId),
        new TypeReference<AggregateState>() {});
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

  private <T> ApiResult<T> checkRouting(String aggregateId, boolean forwarded, String path, TypeReference<T> responseType) {
    KafkaStreams streams = eventify.getKafkaStreams();

    if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
      log.warn("Kafka Streams is not running");
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

    if (forwarded) {
      log.warn("Aggregate {} not owned by this node after forwarding, possible rebalance", aggregateId);
      return new ApiResult.Unavailable<>("Not owned by this node after forwarding");
    }

    return forward(aggregateId, activeHost, path, responseType);
  }

  private <T> ApiResult<T> forward(String aggregateId, HostInfo target, String path, TypeReference<T> responseType) {
    try {
      String separator = path.contains("?") ? "&" : "?";
      String url = "http://" + target.host() + ":" + target.port() + path + separator + "forwarded=true";
      log.debug("Forwarding request for aggregate {} to {}", aggregateId, target);

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(READ_TIMEOUT)
          .GET()
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      int status = response.statusCode();
      if (status == 200) {
        return new ApiResult.Ok<>(objectMapper.readValue(response.body(), responseType));
      } else if (status == 404) {
        return new ApiResult.NotFound<>();
      } else {
        log.warn("Remote node {} returned {} for aggregate {}", target, status, aggregateId);
        return new ApiResult.RemoteError<>(status);
      }
    } catch (IOException | InterruptedException e) {
      log.warn("Failed to forward request for aggregate {} to {}", aggregateId, target, e);
      return new ApiResult.Unavailable<>("Failed to reach remote node");
    }
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

  private String buildEventsQuery(String cursor, int limit) {
    StringBuilder sb = new StringBuilder("?limit=").append(limit);
    if (cursor != null) sb.append("&cursor=").append(URLEncoder.encode(cursor, java.nio.charset.StandardCharsets.UTF_8));
    return sb.toString();
  }

  private String buildStateQuery(String eventId) {
    return eventId != null ? "?eventId=" + URLEncoder.encode(eventId, java.nio.charset.StandardCharsets.UTF_8) : "";
  }
}
