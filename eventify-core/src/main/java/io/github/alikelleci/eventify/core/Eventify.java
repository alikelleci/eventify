package io.github.alikelleci.eventify.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandProcessor;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.core.messaging.eventhandling.EventProcessor;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.core.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.core.support.CustomRocksDbConfig;
import io.github.alikelleci.eventify.core.support.LoggingStateRestoreListener;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerde;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import io.github.alikelleci.eventify.core.util.AnnotationUtils;
import io.github.alikelleci.eventify.core.util.HandlerUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.alikelleci.eventify.core.messaging.Metadata.REPLY_TO;

@Slf4j
@Getter
public class Eventify {
  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers = new HashMap<>();
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<String, Upcaster> upcasters = new ArrayListValuedHashMap<>();

  private final Properties streamsConfig;
  private final StateListener stateListener;
  private final StateRestoreListener stateRestoreListener;
  private final StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
  private final ObjectMapper objectMapper;

  private KafkaStreams writeStreams;
  private KafkaStreams readStreams;

  protected Eventify(Properties streamsConfig,
                     StateListener stateListener,
                     StateRestoreListener stateRestoreListener,
                     StreamsUncaughtExceptionHandler uncaughtExceptionHandler,
                     ObjectMapper objectMapper) {
    this.streamsConfig = streamsConfig;
    this.stateListener = stateListener;
    this.stateRestoreListener = stateRestoreListener;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    this.objectMapper = objectMapper;
  }

  public static EventifyBuilder builder() {
    return new EventifyBuilder();
  }

  public Topology writeTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    Serde<Command> commandSerde = new JsonSerde<>(Command.class, objectMapper);
    Serde<Event> eventSerde = new JsonSerde<>(Event.class, objectMapper, upcasters);
    Serde<AggregateState> snapshotSerde = new JsonSerde<>(AggregateState.class, objectMapper);

    // Event store
    builder.addStateStore(Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("event-store"), Serdes.String(), eventSerde)
        .withLoggingEnabled(Collections.emptyMap()));

    // Snapshot store
    builder.addStateStore(Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("snapshot-store"), Serdes.String(), snapshotSerde)
        .withLoggingEnabled(Collections.emptyMap()));

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
                .withStreamPartitioner((topic, key, value, numPartitions) -> Optional.of(Set.of(0))));

    // Events --> Push
    commandResults
        .filter((key, result) -> result instanceof Success)
        .mapValues((key, result) -> (Success) result)
        .flatMapValues(Success::getEvents)
        .filter((key, event) -> event != null)
        .to((key, event, recordContext) -> event.getTopicInfo().value(),
            Produced.with(Serdes.String(), eventSerde));

    return builder.build();
  }

  public Topology readTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    Serde<Event> eventSerde = new JsonSerde<>(Event.class, objectMapper, upcasters);

    // --> Events
    KStream<String, Event> events = builder.stream(getEventTopics(), Consumed.with(Serdes.String(), eventSerde))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getPayload() != null);

    // Events --> Void
    events.processValues(() -> new EventProcessor(this));

    return builder.build();
  }

  public void start() {
    String appId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
    if (appId == null) {
      throw new IllegalStateException("application.id is required in streamsConfig.");
    }

    if (!getCommandTopics().isEmpty()) {
      Properties writeConfig = new Properties();
      writeConfig.putAll(streamsConfig);
      writeConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId + "-write");

      writeStreams = new KafkaStreams(writeTopology(), writeConfig);
      setUpListeners(writeStreams);
      log.info("Eventify write topology starting...");
      writeStreams.start();
    }

    if (!getEventTopics().isEmpty()) {
      Properties readConfig = new Properties();
      readConfig.putAll(streamsConfig);
      readConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId + "-read");

      readStreams = new KafkaStreams(readTopology(), readConfig);
      setUpListeners(readStreams);
      log.info("Eventify read topology starting...");
      readStreams.start();
    }

    if (writeStreams == null && readStreams == null) {
      log.info("Eventify is not started: no handlers registered.");
      return;
    }

    registerShutdownHook();
  }

  public void stop() {
    log.info("Eventify is shutting down...");
    if (writeStreams != null) writeStreams.close(Duration.ofSeconds(60));
    if (readStreams != null) readStreams.close(Duration.ofSeconds(60));
    log.info("Eventify shut down complete.");
  }

  private void setUpListeners(KafkaStreams streams) {
    streams.setStateListener(this.stateListener);
    streams.setGlobalStateRestoreListener(this.stateRestoreListener);
    streams.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);
  }

  private void registerShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
  }

  private Set<String> getCommandTopics() {
    return commandHandlers.keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private Set<String> getEventTopics() {
    return eventHandlers.keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }


  public static class EventifyBuilder {
    private final List<Object> handlers = new ArrayList<>();

    private Properties streamsConfig;
    private StateListener stateListener;
    private StateRestoreListener stateRestoreListener;
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
    private ObjectMapper objectMapper;

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
      this.streamsConfig.putIfAbsent(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
      this.streamsConfig.putIfAbsent(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDbConfig.class);
      this.streamsConfig.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");
      return this;
    }

    public EventifyBuilder stateListener(StateListener stateListener) {
      this.stateListener = stateListener;
      return this;
    }

    public EventifyBuilder stateRestoreListener(StateRestoreListener stateRestoreListener) {
      this.stateRestoreListener = stateRestoreListener;
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

    public Eventify build() {
      if (this.stateListener == null) {
        this.stateListener = (newState, oldState) ->
            log.info("State changed from {} to {}", oldState, newState);
      }

      if (this.stateRestoreListener == null) {
        this.stateRestoreListener = new LoggingStateRestoreListener();
      }

      if (this.uncaughtExceptionHandler == null) {
        this.uncaughtExceptionHandler = throwable ->
            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
      }

      if (this.objectMapper == null) {
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      Eventify eventify = new Eventify(
          this.streamsConfig,
          this.stateListener,
          this.stateRestoreListener,
          this.uncaughtExceptionHandler,
          this.objectMapper);

      this.handlers.forEach(handler ->
          HandlerUtils.registerHandler(eventify, handler));

      return eventify;
    }
  }
}
