package io.github.alikelleci.eventify.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateDefinitions;
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
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class Eventify implements PluginContext {
  private final HandlerRegistry handlers = new HandlerRegistry();

  private final Properties streamsConfig;
  private final StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
  private final ObjectMapper objectMapper;
  private final List<EventifyPlugin> plugins = new ArrayList<>();

  private KafkaStreams kafkaStreams;
  /** The plugins of this run, as registered when it started. */
  private PluginListeners pluginListeners;
  /** Whether this run is stopped already: {@link #stop()} is called by the application and by the shutdown hook. */
  private final AtomicBoolean stopped = new AtomicBoolean();
  /** Completed after Kafka Streams and all plugins of this run have stopped. */
  private CompletableFuture<Void> stopCompletion = CompletableFuture.completedFuture(null);
  /** Stops this run when the JVM exits without {@link #stop()}; one per run, removed again by {@link #stop()}. */
  private Thread shutdownHook;
  /** How long a close attempt waits before reporting the handler that still runs and trying again. */
  private static final Duration CLOSE_WAIT = Duration.ofSeconds(30);

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
    handlers.register(handler);
  }

  /** Whether objects of this class are handlers: a method has {@code @HandleCommand}, {@code @ApplyEvent}, {@code @HandleEvent} or {@code @Upcast}. */
  public static boolean isHandler(Class<?> type) {
    return HandlerRegistry.isHandler(type);
  }

  public HandlerRegistry getHandlers() {
    return handlers;
  }

  /** The upcasters of the registered handlers: Eventify reads its events with them, and so can a serde of the application. */
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

  /** Whether any handler is registered: a command handler, an event sourcing handler, an event handler or an upcaster. */
  public boolean hasHandlers() {
    return !handlers.isEmpty();
  }

  /** The command classes this instance has a command handler for. */
  @Override
  public Set<Class<?>> getCommandTypes() {
    return handlers.commandHandlers().keySet();
  }

  /** The topics of the commands of one aggregate. */
  @Override
  public Set<String> getCommandTopics(String aggregateType) {
    return handlers.commandTopics(aggregateType);
  }

  /** The names of the aggregates this instance handles, as their {@code @AggregateRoot} gives them. */
  @Override
  public Set<String> getAggregateTypes() {
    return handlers.aggregateTypes();
  }

  /**
   * The public read model of this instance's aggregates, replayed with its event sourcing handlers. Only locally owned
   * aggregates can be read; use {@link #getAggregateMetadata} to locate their owner.
   *
   * @throws IllegalStateException when Eventify has not started
   * @throws org.apache.kafka.streams.errors.InvalidStateStoreException when the stores cannot be read, e.g. during rebalancing
   */
  @Override
  public AggregateRepository getAggregateRepository() {
    return new AggregateRepository(
        new EventStore(runningKafkaStreams().store(StoreQueryParameters.fromNameAndType(StoreNames.EVENT_STORE, QueryableStoreTypes.keyValueStore()))),
        new SnapshotStore(runningKafkaStreams().store(StoreQueryParameters.fromNameAndType(StoreNames.SNAPSHOT_STORE, QueryableStoreTypes.keyValueStore()))),
        handlers.eventSourcingHandlers(), new AggregateDefinitions(handlers.aggregateClasses()));
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

  public void registerPlugin(EventifyPlugin plugin) {
    this.plugins.add(plugin);
  }

  public static EventifyBuilder builder() {
    return new EventifyBuilder();
  }

  public Topology topology() {
    return EventifyTopology.build(handlers, objectMapper);
  }

  public synchronized void start() {
    if (kafkaStreams != null) {
      if (!stopped.get()) {
        throw new IllegalStateException("Eventify is already started.");
      }
      if (!stopCompletion.isDone()) {
        throw new IllegalStateException("Eventify is still stopping.");
      }
    }
    handlers.freeze();
    Topology topology = topology();
    if (topology.describe().subtopologies().isEmpty()) {
      log.info("Eventify is not started: consumer is not subscribed to any topics or assigned any partitions");
      return;
    }

    warnAboutHandlersThatStopEachOther();

    kafkaStreams = new KafkaStreams(topology, streamsConfig);
    stopped.set(false);
    stopCompletion = new CompletableFuture<>();
    pluginListeners = new PluginListeners(plugins);
    setUpListeners();

    log.info("Eventify is starting...");
    kafkaStreams.start();
    pluginListeners.notifyPlugins("onStart", plugin -> plugin.onStart(this));
  }

  /**
   * An exception from an event handler stops Kafka Streams, and command handling stops with it: they are
   * best run in their own application, with their own application id.
   */
  private void warnAboutHandlersThatStopEachOther() {
    if (handlers.commandHandlers().isEmpty() || !handlers.hasEventHandlers()) {
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
   * <p>The application (e.g. Spring, when its context closes) and the shutdown hook can call this at the same time.
   * The one that comes second waits until the first has closed Kafka Streams, so it never returns while handlers still
   * run, e.g. before Spring closes the beans those handlers use. A stream thread itself cannot wait for that close: it
   * is one of the threads Kafka Streams has to join. It starts the close on another thread and returns to finish its
   * handler; that thread stops the plugins only after Kafka Streams has joined every stream thread.
   */
  public void stop() {
    Runnable stop = null;
    CompletableFuture<Void> completion;
    synchronized (this) {
      if (kafkaStreams == null) {
        return;
      }
      completion = stopCompletion;
      if (!stopped.compareAndSet(false, true)) {
        if (isCallingStreamThread()) {
          return;
        }
      } else {
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

  /** Waits until every Kafka Streams thread has stopped, so plugins can safely release their resources afterwards. */
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
      // The JVM is shutting down already: the hook runs anyway, and finds this run stopped.
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
      if (this.uncaughtExceptionHandler == null) {
        this.uncaughtExceptionHandler = throwable ->
            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
      }

      if (this.objectMapper == null) {
        this.objectMapper = EventifyObjectMapper.create();
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
