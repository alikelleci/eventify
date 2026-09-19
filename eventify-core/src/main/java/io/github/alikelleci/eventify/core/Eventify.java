package io.github.alikelleci.eventify.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateReplayer;
import io.github.alikelleci.eventify.core.handler.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.kafka.internal.EventifyTopology;
import io.github.alikelleci.eventify.core.kafka.internal.StreamsConfigDefaults;
import io.github.alikelleci.eventify.core.plugin.EventifyPlugin;
import io.github.alikelleci.eventify.core.plugin.LoggingPlugin;
import io.github.alikelleci.eventify.core.plugin.PluginContext;
import io.github.alikelleci.eventify.core.plugin.internal.PluginListeners;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.store.ReadOnlyEventStore;
import io.github.alikelleci.eventify.core.store.ReadOnlySnapshotStore;
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

  /** The topics of the commands this instance handles. */
  @Override
  public Set<String> getCommandTopics() {
    return handlers.commandTopics();
  }

  /** Rebuilds the state of an aggregate from its events, with the event sourcing handlers of this instance. */
  @Override
  public AggregateReplayer getAggregateReplayer() {
    return new AggregateReplayer(handlers.eventSourcingHandlers());
  }

  /**
   * The events stored on this instance: only those of the aggregates it owns (see {@link #getAggregateMetadata}).
   *
   * @throws org.apache.kafka.streams.errors.InvalidStateStoreException when the store can't be read right now, e.g.
   *                                                                    while Kafka Streams is rebalancing
   */
  @Override
  public ReadOnlyEventStore getEventStore() {
    return ReadOnlyEventStore.of(runningKafkaStreams().store(
        StoreQueryParameters.fromNameAndType(StoreNames.EVENT_STORE, QueryableStoreTypes.keyValueStore())));
  }

  /**
   * The snapshots stored on this instance: only those of the aggregates it owns.
   *
   * @throws org.apache.kafka.streams.errors.InvalidStateStoreException when the store can't be read right now
   */
  @Override
  public ReadOnlySnapshotStore getSnapshotStore() {
    return ReadOnlySnapshotStore.of(runningKafkaStreams().store(
        StoreQueryParameters.fromNameAndType(StoreNames.SNAPSHOT_STORE, QueryableStoreTypes.keyValueStore())));
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
    handlers.freeze();
    Topology topology = topology();
    if (topology.describe().subtopologies().isEmpty()) {
      log.info("Eventify is not started: consumer is not subscribed to any topics or assigned any partitions");
      return;
    }

    warnAboutHandlersThatStopEachOther();

    kafkaStreams = new KafkaStreams(topology, streamsConfig);
    stopped.set(false);
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
    pluginListeners.notifyPlugins("onStop", plugin -> plugin.onStop(this));
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
