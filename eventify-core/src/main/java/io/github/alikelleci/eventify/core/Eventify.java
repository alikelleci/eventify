package io.github.alikelleci.eventify.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateRepository;
import io.github.alikelleci.eventify.core.aggregate.SnapshotStore;
import io.github.alikelleci.eventify.core.handler.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.kafka.internal.EventifyTopology;
import io.github.alikelleci.eventify.core.kafka.internal.StreamsConfigDefaults;
import io.github.alikelleci.eventify.core.plugin.EventifyPlugin;
import io.github.alikelleci.eventify.core.plugin.LoggingPlugin;
import io.github.alikelleci.eventify.core.plugin.PluginContext;
import io.github.alikelleci.eventify.core.plugin.internal.PluginListeners;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.store.EventStore;
import io.github.alikelleci.eventify.core.store.internal.StoreNames;
import io.github.alikelleci.eventify.core.upcasting.Upcasters;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.QueryableStoreTypes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class Eventify implements PluginContext {
  private final HandlerRegistry handlers;

  private final Properties streamsConfig;
  private final StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
  private final ObjectMapper objectMapper;
  private final List<EventifyPlugin> plugins;

  private KafkaStreams kafkaStreams;
  /** The plugins of this run, as registered when it started. */
  private PluginListeners pluginListeners;
  /** Whether this run is stopped: both the application and the shutdown hook call {@link #stop()}. */
  private boolean stopped;
  /** Completed after Kafka Streams and all plugins of this run have stopped. */
  private CompletableFuture<Void> stopCompletion = CompletableFuture.completedFuture(null);
  /** Stops this run when the JVM exits without {@link #stop()}. */
  private Thread shutdownHook;
  /** How long a close attempt waits before logging and trying again. */
  private static final Duration CLOSE_WAIT = Duration.ofSeconds(30);

  protected Eventify(HandlerRegistry handlers,
                     Properties streamsConfig,
                     StreamsUncaughtExceptionHandler uncaughtExceptionHandler,
                     ObjectMapper objectMapper,
                     List<EventifyPlugin> plugins) {
    this.handlers = handlers;
    this.streamsConfig = streamsConfig;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    this.objectMapper = objectMapper;
    this.plugins = List.copyOf(plugins);
  }

  /** Whether a method has {@code @HandleCommand}, {@code @ApplyEvent}, {@code @HandleEvent} or {@code @Upcast}. */
  public static boolean isHandler(Class<?> handlerClass) {
    return HandlerRegistry.isHandler(handlerClass);
  }

  public HandlerRegistry getHandlers() {
    return handlers;
  }

  /** The upcasters of the registered handlers, also usable by an application's own serde. */
  public Upcasters getUpcasters() {
    return handlers.upcasters();
  }

  @Override
  public Properties getStreamsConfig() {
    return streamsConfig;
  }

  @Override
  public ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  /** The running Kafka Streams; {@code null} before the first {@link #start()}. */
  @Override
  public KafkaStreams getKafkaStreams() {
    return kafkaStreams;
  }

  /** The command classes this instance has a command handler for. */
  @Override
  public Set<Class<?>> getCommandClasses() {
    return handlers.commandHandlers().keySet();
  }

  /** The topics of the commands of one aggregate. */
  @Override
  public Set<String> getCommandTopics(String aggregateType) {
    return handlers.commandTopics(aggregateType);
  }

  /** The {@code @AggregateRoot} names of the aggregates this instance handles. */
  @Override
  public Set<String> getAggregateTypes() {
    return handlers.aggregateTypes();
  }

  /**
   * Read model of the locally owned aggregates; select a type with {@link AggregateRepository#forType(String)}.
   * Throws IllegalStateException before start, InvalidStateStoreException while the stores can't be read.
   */
  @Override
  public AggregateRepository getAggregateRepository() {
    return new AggregateRepository(
        new EventStore(runningKafkaStreams().store(StoreQueryParameters.fromNameAndType(StoreNames.EVENT_STORE, QueryableStoreTypes.keyValueStore()))),
        new SnapshotStore(runningKafkaStreams().store(StoreQueryParameters.fromNameAndType(StoreNames.SNAPSHOT_STORE, QueryableStoreTypes.keyValueStore()))),
        handlers.eventSourcingHandlers(), handlers.aggregateClasses());
  }

  /** Which instance of the application owns the aggregate, and so has its events; {@code null} when unknown. */
  @Override
  public KeyQueryMetadata getAggregateMetadata(String aggregateId) {
    return runningKafkaStreams().queryMetadataForKey(StoreNames.EVENT_STORE, aggregateId, Serdes.String().serializer());
  }

  private KafkaStreams runningKafkaStreams() {
    if (kafkaStreams == null) {
      throw new IllegalStateException("Eventify is not started");
    }
    return kafkaStreams;
  }

  public static EventifyBuilder builder() {
    return new EventifyBuilder();
  }

  public Topology topology() {
    return EventifyTopology.build(handlers, objectMapper);
  }

  public synchronized void start() {
    if (kafkaStreams != null) {
      if (!stopped) {
        throw new IllegalStateException("Eventify is already started.");
      }
      if (!stopCompletion.isDone()) {
        throw new IllegalStateException("Eventify is still stopping.");
      }
    }
    Topology topology = topology();
    if (topology.describe().subtopologies().isEmpty()) {
      log.info("Eventify is not started: consumer is not subscribed to any topics or assigned any partitions");
      return;
    }

    warnAboutHandlersThatStopEachOther();

    kafkaStreams = new KafkaStreams(topology, streamsConfig);
    stopped = false;
    stopCompletion = new CompletableFuture<>();
    pluginListeners = new PluginListeners(plugins);
    setUpListeners();

    log.info("Eventify is starting...");
    kafkaStreams.start();
    pluginListeners.notifyPlugins("onStart", plugin -> plugin.onStart(this));
  }

  private void warnAboutHandlersThatStopEachOther() {
    if (handlers.commandHandlers().isEmpty() || !handlers.hasEventHandlers()) {
      return;
    }
    log.warn("This Eventify instance handles commands and events in one application: an exception from an event "
        + "handler stops command handling too. Consider running the event handlers in their own application, "
        + "with its own '{}'.", StreamsConfig.APPLICATION_ID_CONFIG);
  }

  /**
   * Closes Kafka Streams and the plugins, once; a second caller waits until Kafka Streams is closed.
   * From a stream thread the close runs on another thread, since Kafka Streams has to join that thread.
   */
  public void stop() {
    Runnable stop = null;
    CompletableFuture<Void> completion;
    synchronized (this) {
      if (kafkaStreams == null) {
        return;
      }
      completion = stopCompletion;
      if (stopped) {
        if (isCallingStreamThread()) {
          return;
        }
      } else {
        stopped = true;
        removeShutdownHook();
        log.info("Eventify is shutting down...");
        KafkaStreams streams = kafkaStreams;
        PluginListeners listeners = pluginListeners;
        stop = () -> stop(streams, listeners, completion);
        if (isCallingStreamThread()) {
          Thread closeThread = new Thread(stop, "eventify-stop");
          closeThread.start();
          return;
        }
      }
    }

    if (stop != null) {
      stop.run();
      return;
    }
    completion.join();
  }

  /** Closes this run from a thread that can wait for all its stream threads to finish. */
  private void stop(KafkaStreams streams, PluginListeners listeners, CompletableFuture<Void> completion) {
    try {
      closeKafkaStreams(streams);
      listeners.notifyPlugins("onStop", plugin -> plugin.onStop(this));
      log.info("Eventify shut down complete.");
      completion.complete(null);
    } catch (RuntimeException | Error e) {
      completion.completeExceptionally(e);
      throw e;
    }
  }

  /** Waits until every stream thread has stopped, so plugins can release their resources afterwards. */
  void closeKafkaStreams(KafkaStreams streams) {
    while (!streams.close(CLOSE_WAIT)) {
      log.warn("Eventify is still shutting down: Kafka Streams did not stop yet, a handler is still running.");
    }
  }

  /** Uses Kafka Streams' public thread metadata, avoiding a dependency on its internal StreamThread class. */
  boolean isCallingStreamThread() {
    String threadName = Thread.currentThread().getName();
    return kafkaStreams.metadataForLocalThreads().stream()
        .anyMatch(thread -> threadName.equals(thread.threadName()));
  }

  private void removeShutdownHook() {
    if (shutdownHook == null || Thread.currentThread() == shutdownHook) {
      return;
    }
    try {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    } catch (IllegalStateException e) {
      // The JVM is already shutting down: the hook runs anyway.
    }
    shutdownHook = null;
  }

  private void setUpListeners() {
    kafkaStreams.setStateListener(pluginListeners.stateListener());
    kafkaStreams.setGlobalStateRestoreListener(pluginListeners.stateRestoreListener());
    kafkaStreams.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);

    shutdownHook = new Thread(this::stop, "eventify-shutdown");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  public static class EventifyBuilder {
    private final List<Object> handlers = new ArrayList<>();
    private final List<EventifyPlugin> plugins = new ArrayList<>();

    private Properties streamsConfig;
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
    private ObjectMapper objectMapper;

    /** The same object registered twice counts once. */
    public EventifyBuilder registerHandler(Object handler) {
      if (handlers.stream().noneMatch(registered -> registered == handler)) {
        handlers.add(handler);
      }
      return this;
    }

    public EventifyBuilder registerPlugin(EventifyPlugin plugin) {
      plugins.add(plugin);
      return this;
    }

    public EventifyBuilder streamsConfig(Properties streamsConfig) {
      this.streamsConfig = streamsConfig;
      StreamsConfigDefaults.apply(streamsConfig);

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
      List<EventifyPlugin> allPlugins = new ArrayList<>();
      allPlugins.add(new LoggingPlugin());
      allPlugins.addAll(plugins);

      return new Eventify(
          new HandlerRegistry(handlers),
          streamsConfig,
          uncaughtExceptionHandler != null ? uncaughtExceptionHandler
              : throwable -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT,
          objectMapper != null ? objectMapper : EventifyObjectMapper.create(),
          allPlugins);
    }

  }

}
