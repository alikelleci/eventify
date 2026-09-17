package io.github.alikelleci.eventify.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.plugins.EventifyPlugin;
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
import io.github.alikelleci.eventify.core.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.core.messaging.resulthandling.ResultProcessor;
import io.github.alikelleci.eventify.core.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.core.plugins.LoggingPlugin;
import io.github.alikelleci.eventify.core.support.CustomRocksDbConfig;
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
import org.apache.kafka.common.TopicPartition;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.github.alikelleci.eventify.core.messaging.Metadata.REPLY_TO;

@Slf4j
@Getter
public class Eventify {
  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers = new HashMap<>();
  private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<String, Upcaster> upcasters = new ArrayListValuedHashMap<>();

  private final Properties streamsConfig;
  private final StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
  private final ObjectMapper objectMapper;
  private final List<EventifyPlugin> plugins = new ArrayList<>();

  private KafkaStreams kafkaStreams;
  /** Whether this run is stopped already: {@link #stop()} is called by the application and by the shutdown hook. */
  private final AtomicBoolean stopped = new AtomicBoolean();
  /** Stops this run when the JVM exits without {@link #stop()}; one per run, removed again by {@link #stop()}. */
  private Thread shutdownHook;

  protected Eventify(Properties streamsConfig,
                     StreamsUncaughtExceptionHandler uncaughtExceptionHandler,
                     ObjectMapper objectMapper,
                     List<EventifyPlugin> plugins) {
    this.streamsConfig = streamsConfig;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    this.objectMapper = objectMapper;
    this.plugins.addAll(plugins);
  }

  public void registerHandler(Object handler) {
    HandlerUtils.registerHandler(this, handler);
  }

  public void registerPlugin(EventifyPlugin plugin) {
    this.plugins.add(plugin);
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
    Serde<AggregateState> snapshotSerde = new JsonSerde<>(AggregateState.class, objectMapper);

    /*
     * -------------------------------------------------------------
     * STORES
     * -------------------------------------------------------------
     */

    // Event store
    builder.addStateStore(Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("event-store"), Serdes.String(), eventSerde)
        .withLoggingEnabled(Collections.emptyMap()));

    // Snapshot Store
    builder.addStateStore(Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("snapshot-store"), Serdes.String(), snapshotSerde)
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
                  .withStreamPartitioner((topic, key, value, numPartitions) -> Optional.of(Set.of(0))));

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
          .processValues(() -> new EventProcessor(this));
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

  public synchronized void start() {
    Topology topology = topology();
    if (topology.describe().subtopologies().isEmpty()) {
      log.info("Eventify is not started: consumer is not subscribed to any topics or assigned any partitions");
      return;
    }

    warnAboutHandlersThatStopEachOther();

    kafkaStreams = new KafkaStreams(topology, streamsConfig);
    stopped.set(false);
    setUpListeners();

    log.info("Eventify is starting...");
    kafkaStreams.start();
    notifyListeners(plugins, "onStart", plugin -> plugin.onStart(this));
  }

  /**
   * An exception from an event or result handler stops Kafka Streams, and command handling stops with it: they are
   * best run in their own application, with their own application id.
   */
  private void warnAboutHandlersThatStopEachOther() {
    if (commandHandlers.isEmpty() || (eventHandlers.isEmpty() && resultHandlers.isEmpty())) {
      return;
    }
    log.warn("This Eventify instance handles commands and events in one application: an exception from an event "
        + "handler stops command handling too. Consider running the event handlers in their own application, "
        + "with its own '{}'.", StreamsConfig.APPLICATION_ID_CONFIG);
  }

  /**
   * Closes Kafka Streams and stops the plugins, once. Also after Kafka Streams stopped by itself (e.g. in ERROR): its
   * resources and the plugins' still need to be released.
   *
   * <p>Synchronized: the application (e.g. Spring, when its context closes) and the shutdown hook can call this at the
   * same time. The one that comes second waits until the first has closed Kafka Streams, so it never returns while
   * handlers still run, e.g. before Spring closes the beans those handlers use.
   */
  public synchronized void stop() {
    if (kafkaStreams == null || !stopped.compareAndSet(false, true)) {
      return;
    }
    removeShutdownHook();
    log.info("Eventify is shutting down...");
    kafkaStreams.close(Duration.ofSeconds(30));
    notifyListeners(plugins, "onStop", plugin -> plugin.onStop(this));
    log.info("Eventify shut down complete.");
  }

  private void removeShutdownHook() {
    if (shutdownHook == null || Thread.currentThread() == shutdownHook) {
      return;
    }
    try {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    } catch (IllegalStateException e) {
      // The JVM is shutting down already: the hook runs anyway, and finds this run stopped.
    }
    shutdownHook = null;
  }

  /** Kafka Streams takes one listener of each kind, so one listener passes everything on to the plugins that asked. */
  private void setUpListeners() {
    List<StateListener> stateListeners = listenersOf(EventifyPlugin::stateListener);
    List<StateRestoreListener> restoreListeners = listenersOf(EventifyPlugin::stateRestoreListener);

    kafkaStreams.setStateListener((newState, oldState) ->
        notifyListeners(stateListeners, "onChange", listener -> listener.onChange(newState, oldState)));

    kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
      @Override
      public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        notifyListeners(restoreListeners, "onRestoreStart", listener -> listener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset));
      }

      @Override
      public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        notifyListeners(restoreListeners, "onBatchRestored", listener -> listener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored));
      }

      @Override
      public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        notifyListeners(restoreListeners, "onRestoreEnd", listener -> listener.onRestoreEnd(topicPartition, storeName, totalRestored));
      }

      @Override
      public void onRestoreSuspended(TopicPartition topicPartition, String storeName, long totalRestored) {
        notifyListeners(restoreListeners, "onRestoreSuspended", listener -> listener.onRestoreSuspended(topicPartition, storeName, totalRestored));
      }
    });

    kafkaStreams.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);

    shutdownHook = new Thread(this::stop, "eventify-shutdown");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  private <T> List<T> listenersOf(Function<EventifyPlugin, T> listener) {
    return plugins.stream().map(listener).filter(Objects::nonNull).toList();
  }

  /** Tells every listener or plugin, on the calling thread. One that throws is logged and skipped. */
  private static <T> void notifyListeners(List<T> listeners, String hook, Consumer<T> call) {
    listeners.forEach(listener -> {
      try {
        call.accept(listener);
      } catch (Exception e) {
        log.warn("Plugin {} failed in {}", listener.getClass().getName(), hook, e);
      }
    });
  }

  public Set<String> getCommandTopics() {
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

  private Set<String> getResultTopics() {
    return resultHandlers.keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .map(topic -> topic.concat(".results"))
        .collect(Collectors.toSet());
  }


  public static class EventifyBuilder {
    private final List<Object> handlers = new ArrayList<>();
    private final List<EventifyPlugin> plugins = new ArrayList<>();

    private Properties streamsConfig;
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
    private ObjectMapper objectMapper;

    public EventifyBuilder registerHandler(Object handler) {
      handlers.add(handler);
      return this;
    }

    public EventifyBuilder registerPlugin(EventifyPlugin plugin) {
      plugins.add(plugin);
      return this;
    }

    public EventifyBuilder streamsConfig(Properties streamsConfig) {
      this.streamsConfig = streamsConfig;
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      // Always exactly-once: a command's events, its result and the event store are written in one transaction. With
      // at-least-once, a command handled again after a crash would add its events a second time, under other ids.
      Object guarantee = this.streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
      if (guarantee != null && !StreamsConfig.EXACTLY_ONCE_V2.equals(guarantee)) {
        log.warn("'{}' is set by Eventify to '{}'; the configured value '{}' is not used.", StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2, guarantee);
      }
      this.streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
      this.streamsConfig.putIfAbsent(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
      this.streamsConfig.putIfAbsent(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDbConfig.class);
      this.streamsConfig.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");

      // A unique name for this instance, not an address: nothing listens on it. Kafka Streams shares it with the
      // other instances, so each one can tell which instance owns a key (used by the console to route queries).
      // Always set here: two instances with the same name would be taken for one.
      String applicationId = this.streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG, "eventify");
      Object configured = this.streamsConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationId + "." + UUID.randomUUID() + ":0");
      if (configured != null) {
        log.warn("'{}' is set by Eventify; the configured value '{}' is not used.", StreamsConfig.APPLICATION_SERVER_CONFIG, configured);
      }

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//
//    this.streamsConfig.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG), interceptors);

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
      if (this.uncaughtExceptionHandler == null) {
        this.uncaughtExceptionHandler = throwable ->
            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
      }

      if (this.objectMapper == null) {
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      // What happens underneath is logged by a plugin, so it can be seen, replaced or joined by others.
      this.plugins.add(0, new LoggingPlugin());

      Eventify eventify = new Eventify(
          this.streamsConfig,
          this.uncaughtExceptionHandler,
          this.objectMapper,
          this.plugins);

      this.handlers.forEach(eventify::registerHandler);

      return eventify;
    }

  }

}
