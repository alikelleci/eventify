package io.github.alikelleci.eventify;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandProcessor;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.messaging.eventhandling.EventProcessor;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultProcessor;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.support.CustomRocksDbConfig;
import io.github.alikelleci.eventify.support.serializer.JsonSerde;
import io.github.alikelleci.eventify.util.HandlerUtils;
import io.github.alikelleci.eventify.util.JacksonUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.Stores;
import org.springframework.core.annotation.AnnotationUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.alikelleci.eventify.messaging.Metadata.REPLY_TO;

@Slf4j
@Getter
public class Eventify {
  private final MultiValuedMap<String, Upcaster> upcasters = new ArrayListValuedHashMap<>();
  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers = new HashMap<>();
  private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();

  private final Properties streamsConfig;
  private final StateListener stateListener;
  private final StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
  private final ObjectMapper objectMapper;
  private final boolean deleteEventsOnSnapshot;

  private KafkaStreams kafkaStreams;

  protected Eventify(Properties streamsConfig,
                     StateListener stateListener,
                     StreamsUncaughtExceptionHandler uncaughtExceptionHandler,
                     ObjectMapper objectMapper,
                     boolean deleteEventsOnSnapshot) {
    this.streamsConfig = streamsConfig;
    this.stateListener = stateListener;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    this.objectMapper = objectMapper;
    this.deleteEventsOnSnapshot = deleteEventsOnSnapshot;
  }

  public static EventifyBuilder builder() {
    return new EventifyBuilder();
  }

  public Topology topology() {
    StreamsBuilder builder = new StreamsBuilder();

    /*
     * -------------------------------------------------------------
     * SERDES
     * -------------------------------------------------------------
     */

    Serde<Command> commandSerde = new JsonSerde<>(Command.class, objectMapper);
    Serde<Event> eventSerde = new JsonSerde<>(Event.class, objectMapper, upcasters);
    Serde<Aggregate> snapshotSerde = new JsonSerde<>(Aggregate.class, objectMapper);

    /*
     * -------------------------------------------------------------
     * STORES
     * -------------------------------------------------------------
     */

    // Event store
    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("event-store"), Serdes.String(), eventSerde)
        .withLoggingEnabled(Collections.emptyMap()));

    // Snapshot Store
    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("snapshot-store"), Serdes.String(), snapshotSerde)
        .withLoggingEnabled(Collections.emptyMap()));

    /*
     * -------------------------------------------------------------
     * COMMAND HANDLING
     * -------------------------------------------------------------
     */

    if (!getCommandTopics().isEmpty()) {
      // --> Commands
      KStream<String, Command> commands = builder.stream(getCommandTopics(), Consumed.with(Serdes.String(), commandSerde))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null)
          .filter((key, command) -> command.getPayload() != null);

      // Commands --> Results
      KStream<String, CommandResult> commandResults = commands
          .processValues(() -> new CommandProcessor(this), "event-store", "snapshot-store")
          .filter((key, result) -> result != null);

      // Results --> Push
      commandResults
          .mapValues(CommandResult::getCommand)
          .to((key, command, recordContext) -> command.getTopicInfo().value().concat(".results"),
              Produced.with(Serdes.String(), commandSerde));

      // Results --> Push to reply topic
      commandResults
          .mapValues(CommandResult::getCommand)
          .filter((key, command) -> StringUtils.isNotBlank(command.getMetadata().get(REPLY_TO)))
          .to((key, command, recordContext) -> command.getMetadata().get(REPLY_TO),
              Produced.with(Serdes.String(), commandSerde)
                  .withStreamPartitioner((topic, key, value, numPartitions) -> 0));

      // Events --> Push
      commandResults
          .filter((key, result) -> result instanceof Success)
          .mapValues((key, result) -> (Success) result)
          .flatMapValues(Success::getEvents)
          .filter((key, event) -> event != null)
          .to((key, event, recordContext) -> event.getTopicInfo().value(),
              Produced.with(Serdes.String(), eventSerde));
    }

    /*
     * -------------------------------------------------------------
     * EVENT HANDLING
     * -------------------------------------------------------------
     */

    if (!getEventTopics().isEmpty()) {
      // --> Events
      KStream<String, Event> events = builder.stream(getEventTopics(), Consumed.with(Serdes.String(), eventSerde))
          .filter((key, event) -> key != null)
          .filter((key, event) -> event != null)
          .filter((key, event) -> event.getPayload() != null);

      // Events --> Void
      events
          .processValues(() -> new EventProcessor(this), "event-store");
    }

    /*
     * -------------------------------------------------------------
     * RESULT HANDLING
     * -------------------------------------------------------------
     */

    if (!getResultTopics().isEmpty()) {
      // --> Results
      KStream<String, Command> results = builder.stream(getResultTopics(), Consumed.with(Serdes.String(), commandSerde))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null)
          .filter((key, command) -> command.getPayload() != null);

      // Results --> Void
      results
          .processValues(() -> new ResultProcessor(this));
    }


    return builder.build();
  }

  public void start() {
    if (kafkaStreams != null) {
      log.info("Eventify already started.");
      return;
    }

    Topology topology = topology();
    if (topology.describe().subtopologies().isEmpty()) {
      log.info("Eventify is not started: consumer is not subscribed to any topics or assigned any partitions");
      return;
    }

    kafkaStreams = new KafkaStreams(topology, streamsConfig);
    setUpListeners();

    log.info("Eventify is starting...");
    kafkaStreams.start();
  }

  public void stop() {
    if (kafkaStreams == null) {
      log.info("Eventify already stopped.");
      return;
    }

    log.info("Eventify is shutting down...");
    kafkaStreams.close(Duration.ofMillis(5000));
    kafkaStreams = null;
  }

  private void setUpListeners() {
    kafkaStreams.setStateListener(this.stateListener);
    kafkaStreams.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);

    kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
      @Override
      public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        log.debug("State restoration started: topic={}, partition={}, store={}, endingOffset={}", topicPartition.topic(), topicPartition.partition(), storeName, endingOffset);
      }

      @Override
      public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        log.debug("State restoration in progress: topic={}, partition={}, store={}, numRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, numRestored);
      }

      @Override
      public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        log.debug("State restoration ended: topic={}, partition={}, store={}, totalRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Eventify is shutting down...");
      kafkaStreams.close(Duration.ofMillis(5000));
    }));
  }

  private Set<String> getCommandTopics() {
    return commandHandlers.keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private Set<String> getEventTopics() {
    return Stream.of(
            eventHandlers.keySet(),
            eventSourcingHandlers.keySet()
        )
        .flatMap(Collection::stream)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private Set<String> getResultTopics() {
    return resultHandlers.keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .map(topic -> topic.concat(".results"))
        .collect(Collectors.toSet());
  }


  public static class EventifyBuilder {
    private List<Object> handlers = new ArrayList<>();

    private Properties streamsConfig;
    private StateListener stateListener;
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
    private ObjectMapper objectMapper;
    private boolean deleteEventsOnSnapshot;

    public EventifyBuilder registerHandler(Object handler) {
      handlers.add(handler);

      return this;
    }

    public EventifyBuilder streamsConfig(Properties streamsConfig) {
      this.streamsConfig = streamsConfig;
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
      this.streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
      this.streamsConfig.putIfAbsent(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDbConfig.class);
      this.streamsConfig.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//
//    this.streamsConfig.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG), interceptors);

      return this;
    }

    public EventifyBuilder stateListener(StateListener stateListener) {
      this.stateListener = stateListener;
      return this;
    }

    public EventifyBuilder uncaughtExceptionHandler(StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
      this.uncaughtExceptionHandler = uncaughtExceptionHandler;
      return this;
    }

    public EventifyBuilder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public EventifyBuilder deleteEventsOnSnapshot(boolean deleteEventsOnSnapshot) {
      this.deleteEventsOnSnapshot = deleteEventsOnSnapshot;
      return this;
    }

    public Eventify build() {
      if (this.stateListener == null) {
        this.stateListener = (newState, oldState) ->
            log.warn("State changed from {} to {}", oldState, newState);
      }

      if (this.uncaughtExceptionHandler == null) {
        this.uncaughtExceptionHandler = (throwable) ->
            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
      }

      if (this.objectMapper == null) {
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      Eventify eventify = new Eventify(
          this.streamsConfig,
          this.stateListener,
          this.uncaughtExceptionHandler,
          this.objectMapper,
          this.deleteEventsOnSnapshot);

      this.handlers.forEach(handler ->
          HandlerUtils.registerHandler(eventify, handler));

      return eventify;
    }

  }

}
