package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.RawValue;
import io.github.alikelleci.eventify.console.protocol.InstanceStatus;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import io.github.alikelleci.eventify.core.util.HandlerUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.github.alikelleci.eventify.core.messaging.Metadata.REPLY_TO;

@Slf4j
class ConsoleService {

  private static final String EVENT_STORE = "event-store";
  private static final String SNAPSHOT_STORE = "snapshot-store";

  /** On a command retried from the console: the id of the command it retries. */
  static final String RETRY_OF = "$retryOf";

  /** How far back the commands are read. */
  private static final Duration COMMANDS_LOOKBACK = Duration.ofDays(7);
  /** Reading the commands stops after this long, so a read that can't finish doesn't keep a query thread. */
  private static final Duration MAX_COMMANDS_READ = Duration.ofSeconds(60);
  /** How long a retried command may take to reach Kafka before the console is told it failed. */
  private static final Duration RETRY_SEND_TIMEOUT = Duration.ofSeconds(10);

  /**
   * The aggregate's newest commands.
   *
   * @param lookbackDays how far back the commands were read: older commands are not in the page
   * @param truncated    whether more commands were found than the page holds: the oldest ones are left out
   */
  record CommandsPage(List<Command> commands, long lookbackDays, boolean truncated) {}
  record EventsPage(List<Event> events, String nextCursor) {}
  /**
   * An event with the state after and before it. A state is {@code null} when there is none, or when it is unknown:
   * {@code stateKnown} and {@code previousStateKnown} tell which. A state is unknown when the events before it were
   * deleted at a snapshot. The states are {@link AggregateState}s as JSON, see {@link AggregateHistory}.
   */
  record EventDetail(Event event, RawValue state, RawValue previousState,
                     boolean stateKnown, boolean previousStateKnown) {}
  record CommandEventsPage(List<Event> events) {}

  /** The outcome of a query, as the console is told it, with the answer when it's {@link ReplyHeader.Status#OK}. */
  record Result<T>(ReplyHeader header, T value) {
    static <T> Result<T> ok(T value) {
      return new Result<>(ReplyHeader.ok(), value);
    }

    static <T> Result<T> notFound() {
      return new Result<>(ReplyHeader.notFound(), null);
    }

    /** Another instance owns the aggregate; {@code owner} is its node id. */
    static <T> Result<T> notOwner(String owner) {
      return new Result<>(ReplyHeader.notOwner(owner), null);
    }

    static <T> Result<T> unavailable(String reason) {
      return new Result<>(ReplyHeader.unavailable(reason), null);
    }

    static <T> Result<T> badRequest(String reason) {
      return new Result<>(ReplyHeader.badRequest(reason), null);
    }

    boolean isOk() {
      return header.status() == ReplyHeader.Status.OK;
    }
  }

  private final Eventify eventify;
  private final StatusTracker statusTracker;
  private final HostInfo thisHost;
  private final ObjectMapper objectMapper;
  private final Producer<String, Command> producer;
  private final AggregateHistory history;

  ConsoleService(Eventify eventify, StatusTracker statusTracker) {
    this.eventify = eventify;
    this.statusTracker = statusTracker;
    this.objectMapper = eventify.getObjectMapper();
    this.thisHost = hostInfo(eventify);
    this.history = new AggregateHistory(eventify.getEventSourcingHandlers(), objectMapper);
    this.producer = new KafkaProducer<>(producerConfig(eventify), new StringSerializer(), new JsonSerializer<>(objectMapper));
  }

  /**
   * How this instance is doing: its Kafka Streams state, how long it has been in it, and whether it is restoring.
   * Everything is read from what Kafka Streams already keeps in memory: no calls to Kafka, and not on the stream threads.
   */
  Result<InstanceStatus> getStatus() {
    KafkaStreams streams = eventify.getKafkaStreams();
    if (streams == null) {
      return Result.unavailable("Eventify is not started");
    }

    return Result.ok(new InstanceStatus(streams.state().name(), statusTracker.stateForMs(), statusTracker.restoring()));
  }

  void close() {
    producer.close();
  }

  /** This instance's {@code application.server}, which Eventify always sets. */
  static HostInfo hostInfo(Eventify eventify) {
    return HostInfo.buildFromEndpoint(eventify.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_SERVER_CONFIG));
  }

  /** How instances refer to each other: {@link #hostInfo(Eventify)} as {@code host:port}. */
  static String nodeId(HostInfo hostInfo) {
    return hostInfo.host() + ":" + hostInfo.port();
  }

  /**
   * The application's own Kafka client settings (security included, and its {@code producer.} settings), without the
   * ones Kafka Streams only adds for its exactly-once processing.
   */
  static Map<String, Object> producerConfig(Eventify eventify) {
    Map<String, Object> config = new StreamsConfig(eventify.getStreamsConfig()).getProducerConfigs(clientId(eventify, "producer"));
    config.remove(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
    config.remove(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
    config.remove(ProducerConfig.LINGER_MS_CONFIG);
    return config;
  }

  /** The application's own Kafka client settings (security included, and its {@code consumer.} settings), to read a topic without a group. */
  static Map<String, Object> consumerConfig(Eventify eventify) {
    Map<String, Object> config = new StreamsConfig(eventify.getStreamsConfig()).getRestoreConsumerConfigs(clientId(eventify, "commands"));
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    // Every read seeks to where it starts; this only applies when that offset is no longer there.
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    return config;
  }

  private static String clientId(Eventify eventify, String purpose) {
    return eventify.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-console-" + purpose;
  }

  /**
   * Sends the command again, as a new command: with its own id and correlation id, so the events it produces are its
   * own, and {@link #RETRY_OF} pointing to the command it retries.
   *
   * @param json the command as the console received it. Only a command this application handles is accepted, checked
   *             before it is read: the JSON names the class to create.
   */
  Result<Void> retryCommand(byte[] json) {
    Command original;
    try {
      JsonNode tree = objectMapper.readTree(json);
      String type = tree.path("payload").path("@class").asText(null);
      if (!isHandledCommand(type)) {
        return Result.badRequest("Not a command of this application: " + type);
      }
      original = objectMapper.treeToValue(tree, Command.class);
    } catch (Exception e) {
      log.warn("Failed to read the command to retry", e);
      return Result.badRequest("Unreadable command");
    }

    // The correlation id stays: the retry belongs to the same flow (e.g. a saga) as the command it retries, and its
    // events can be traced with the rest of that flow. RETRY_OF tells the retry apart from the original.
    Metadata retryMetadata = Metadata.builder()
        .putAll(original.getMetadata())
        .put(RETRY_OF, original.getId())
        .build();
    retryMetadata.remove(REPLY_TO);

    Command retryCommand = Command.builder()
        .payload(original.getPayload())
        .metadata(retryMetadata)
        .build();

    String commandTopic = original.getTopicInfo().value();

    // Waits until Kafka has the command: only then is the retry done. A send that fails later (no access to the topic,
    // the broker unreachable) would otherwise be reported as done.
    try {
      producer.send(new ProducerRecord<>(commandTopic, null, retryCommand.getTimestamp().toEpochMilli(),
          retryCommand.getAggregateId(), retryCommand)).get(RETRY_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      log.info("Retried command {} as {} on topic {}", original.getId(), retryCommand.getId(), commandTopic);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Result.unavailable("Interrupted while publishing the retry command");
    } catch (Exception e) {
      Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
      log.error("Failed to publish retry command for {}", original.getId(), cause);
      return Result.unavailable("Failed to publish retry command: " + cause.getMessage());
    }

    return Result.ok(null);
  }

  /**
   * Whether the class is a command this application handles: it, or one of its supertypes, has a command handler.
   * The class is looked up without being initialized, before anything of the JSON is read as that class.
   */
  private boolean isHandledCommand(String className) {
    if (className == null) {
      return false;
    }
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader() != null
        ? Thread.currentThread().getContextClassLoader()
        : ConsoleService.class.getClassLoader();
    try {
      Class<?> type = Class.forName(className, false, classLoader);
      return HandlerUtils.findHandler(eventify.getCommandHandlers(), type) != null;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  /**
   * Reads the aggregate's commands from the result topics. Every call has its own consumer, so calls never affect each
   * other. When the request is cancelled, only this call's consumer stops, the way Kafka intends: with a wakeup.
   */
  Result<CommandsPage> getCommands(String aggregateId, int limit, CancelSignal cancel) {
    // Eventify writes the result of every handled command to its command topic with .results.
    Set<String> resultTopics = eventify.getCommandTopics().stream()
        .map(topic -> topic.concat(".results"))
        .collect(Collectors.toSet());
    if (resultTopics.isEmpty()) {
      return Result.ok(new CommandsPage(List.of(), COMMANDS_LOOKBACK.toDays(), false));
    }

    List<Command> results = new ArrayList<>();
    JsonDeserializer<Command> commandDeserializer = new JsonDeserializer<>(Command.class, objectMapper);
    Instant deadline = Instant.now().plus(MAX_COMMANDS_READ);

    // Closed in reverse order: the wakeup is unregistered before the consumer closes.
    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig(eventify), new StringDeserializer(), new ByteArrayDeserializer());
         AutoCloseable stopOnCancel = cancel.onCancel(consumer::wakeup)) {
      for (String topic : resultTopics) {
        try {
          readCommands(consumer, topic, aggregateId, commandDeserializer, deadline, results);
        } catch (WakeupException e) {
          throw e;
        } catch (CommandsReadTimeout e) {
          log.warn("Reading the commands of aggregate {} took longer than {}", aggregateId, MAX_COMMANDS_READ);
          return Result.unavailable("Reading the commands took too long");
        } catch (Exception e) {
          // Not an empty list: that would look like the aggregate has no commands.
          log.warn("Failed to read commands of aggregate {} from topic {}", aggregateId, topic, e);
          return Result.unavailable("Failed to read commands from topic " + topic + ": " + e.getMessage());
        }
      }
    } catch (WakeupException e) {
      log.debug("Stopped reading commands for aggregate {}: the request was cancelled", aggregateId);
      return Result.unavailable("Cancelled");
    } catch (Exception e) {
      log.error("Unexpected error querying commands for aggregate {}", aggregateId, e);
      return Result.unavailable("Unexpected error");
    }

    results.sort((a, b) -> b.getTimestamp().compareTo(a.getTimestamp()));
    boolean truncated = results.size() > limit;
    List<Command> limited = truncated ? results.subList(0, limit) : results;
    return Result.ok(new CommandsPage(limited, COMMANDS_LOOKBACK.toDays(), truncated));
  }

  /**
   * Adds the aggregate's commands of the last {@link #COMMANDS_LOOKBACK} from one result topic: from the partition its
   * key is written to, up to the end of that partition when the read starts.
   */
  private static void readCommands(KafkaConsumer<String, byte[]> consumer, String topic, String aggregateId,
                                   JsonDeserializer<Command> commandDeserializer, Instant deadline, List<Command> results) {
    int numPartitions = consumer.partitionsFor(topic).size();
    if (numPartitions == 0) {
      return;
    }
    // The partition the default partitioner picks for this key, as Eventify writes the results.
    int partition = Utils.toPositive(Utils.murmur2(aggregateId.getBytes(StandardCharsets.UTF_8))) % numPartitions;
    TopicPartition tp = new TopicPartition(topic, partition);
    consumer.assign(List.of(tp));

    long endOffset = consumer.endOffsets(List.of(tp)).getOrDefault(tp, 0L);
    long since = Instant.now().minus(COMMANDS_LOOKBACK).toEpochMilli();
    OffsetAndTimestamp first = consumer.offsetsForTimes(Map.of(tp, since)).get(tp);
    // None: every record is older than the lookback, so there is nothing to read.
    long startOffset = first != null ? first.offset() : endOffset;
    if (startOffset >= endOffset) {
      return;
    }
    consumer.seek(tp, startOffset);

    // Until the position passes the end, not until a poll comes back empty: a slow poll isn't the end, and the last
    // offsets can be transaction markers, which are never returned as records.
    while (consumer.position(tp) < endOffset) {
      if (Instant.now().isAfter(deadline)) {
        throw new CommandsReadTimeout();
      }
      for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofSeconds(1))) {
        if (record.offset() >= endOffset || !aggregateId.equals(record.key()) || record.value() == null) {
          continue;
        }
        try {
          Command command = commandDeserializer.deserialize(topic, record.value());
          if (command != null) {
            results.add(command);
          }
        } catch (Exception e) {
          log.warn("Failed to deserialize command record on topic {} at offset {}", topic, record.offset(), e);
        }
      }
    }
  }

  private static class CommandsReadTimeout extends RuntimeException {
  }

  Result<CommandEventsPage> getEventsOfCommand(String aggregateId, String commandId, String correlationId) {
    Result<CommandEventsPage> routing = checkRouting(aggregateId);
    if (routing != null) return routing;

    try {
      return Result.ok(new CommandEventsPage(history.eventsOfCommand(eventStore(), aggregateId, commandId, correlationId)));
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return Result.unavailable("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying the events of command {} of aggregate {}", commandId, aggregateId, e);
      return Result.unavailable("Unexpected error");
    }
  }

  Result<EventsPage> getEvents(String aggregateId, String cursor, int limit) {
    Result<EventsPage> routing = checkRouting(aggregateId);
    if (routing != null) {
      return routing;
    }

    try {
      return Result.ok(history.events(eventStore(), aggregateId, cursor, limit));
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return Result.unavailable("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying events for aggregate {}", aggregateId, e);
      return Result.unavailable("Unexpected error");
    }
  }

  Result<EventDetail> getEventDetail(String aggregateId, String eventId) {
    Result<EventDetail> routing = checkRouting(aggregateId);
    if (routing != null) {
      return routing;
    }

    try {
      EventDetail detail = history.eventDetail(eventStore(), snapshotStore(), aggregateId, eventId);
      if (detail == null) {
        return notFound(aggregateId);
      }
      return Result.ok(detail);
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return Result.unavailable("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying event detail for aggregate {}", aggregateId, e);
      return Result.unavailable("Unexpected error");
    }
  }

  /** The {@link AggregateState} as JSON, see {@link AggregateHistory}. */
  Result<RawValue> getState(String aggregateId, String eventId) {
    Result<RawValue> routing = checkRouting(aggregateId);
    if (routing != null) {
      return routing;
    }

    try {
      RawValue state = history.stateAt(eventStore(), snapshotStore(), aggregateId, eventId);
      if (state == null) {
        return notFound(aggregateId);
      }
      return Result.ok(state);
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return Result.unavailable("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying state for aggregate {}", aggregateId, e);
      return Result.unavailable("Unexpected error");
    }
  }

  private ReadOnlyKeyValueStore<String, Event> eventStore() {
    return eventify.getKafkaStreams().store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));
  }

  private ReadOnlyKeyValueStore<String, AggregateState> snapshotStore() {
    return eventify.getKafkaStreams().store(StoreQueryParameters.fromNameAndType(SNAPSHOT_STORE, QueryableStoreTypes.keyValueStore()));
  }

  /** Nothing found; unless this instance stopped owning the aggregate during the query, and simply no longer has it. */
  private <T> Result<T> notFound(String aggregateId) {
    if (!isLocallyAuthoritative(aggregateId)) {
      log.debug("Ownership/availability changed while querying aggregate {}; returning 503", aggregateId);
      return Result.unavailable("Ownership changed during query");
    }
    return Result.notFound();
  }

  /**
   * Returns {@code null} when this instance can answer for the aggregate, or the reason it can't: Kafka Streams isn't
   * running, or another instance owns the aggregate. The console then asks that instance instead.
   */
  private <T> Result<T> checkRouting(String aggregateId) {
    KafkaStreams streams = eventify.getKafkaStreams();

    if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
      log.debug("Kafka Streams is not running");
      return Result.unavailable("Kafka Streams is not running");
    }

    KeyQueryMetadata metadata = streams.queryMetadataForKey(EVENT_STORE, aggregateId, Serdes.String().serializer());
    if (metadata == null || metadata.activeHost().equals(HostInfo.unavailable())) {
      log.warn("Metadata unavailable for aggregate {}", aggregateId);
      return Result.unavailable("Metadata unavailable");
    }

    HostInfo activeHost = metadata.activeHost();
    if (activeHost.equals(thisHost)) {
      return null;
    }

    return Result.notOwner(nodeId(activeHost));
  }

  private boolean isLocallyAuthoritative(String aggregateId) {
    KafkaStreams streams = eventify.getKafkaStreams();
    if (streams.state() != KafkaStreams.State.RUNNING) {
      return false;
    }
    KeyQueryMetadata metadata = streams.queryMetadataForKey(EVENT_STORE, aggregateId, Serdes.String().serializer());
    return metadata != null && thisHost.equals(metadata.activeHost());
  }
}
