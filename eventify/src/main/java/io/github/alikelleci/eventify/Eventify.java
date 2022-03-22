package io.github.alikelleci.eventify;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandTransformer;
import io.github.alikelleci.eventify.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.messaging.eventhandling.EventTransformer;
import io.github.alikelleci.eventify.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultTransformer;
import io.github.alikelleci.eventify.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
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

import java.lang.reflect.Method;
import java.time.Duration;
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

@Slf4j
@Getter
public class Eventify {
  private MultiValuedMap<String, Upcaster> upcasters;
  private Map<Class<?>, CommandHandler> commandHandlers;
  private Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;
  private MultiValuedMap<Class<?>, ResultHandler> resultHandlers;
  private MultiValuedMap<Class<?>, EventHandler> eventHandlers;

  private Properties streamsConfig;
  private StateListener stateListener;
  private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
  private boolean deleteEventsOnSnapshot;

  protected Eventify(MultiValuedMap<String, Upcaster> upcasters, Map<Class<?>, CommandHandler> commandHandlers, Map<Class<?>, EventSourcingHandler> eventSourcingHandlers, MultiValuedMap<Class<?>, ResultHandler> resultHandlers, MultiValuedMap<Class<?>, EventHandler> eventHandlers, Properties streamsConfig, StateListener stateListener, StreamsUncaughtExceptionHandler uncaughtExceptionHandler, boolean deleteEventsOnSnapshot) {
    this.upcasters = upcasters;
    this.commandHandlers = commandHandlers;
    this.eventSourcingHandlers = eventSourcingHandlers;
    this.resultHandlers = resultHandlers;
    this.eventHandlers = eventHandlers;
    this.streamsConfig = streamsConfig;
    this.stateListener = stateListener;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    this.deleteEventsOnSnapshot = deleteEventsOnSnapshot;
  }

  public static Builder builder() {
    return new Builder();
  }

  protected Topology topology() {
    StreamsBuilder builder = new StreamsBuilder();

    /*
     * -------------------------------------------------------------
     * STORES
     * -------------------------------------------------------------
     */

    // Event store
    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("event-store"), Serdes.String(), CustomSerdes.Json(Event.class))
        .withLoggingEnabled(Collections.emptyMap()));

    // Snapshot Store
    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("snapshot-store"), Serdes.String(), CustomSerdes.Json(Aggregate.class))
        .withLoggingEnabled(Collections.emptyMap()));

    /*
     * -------------------------------------------------------------
     * COMMAND HANDLING
     * -------------------------------------------------------------
     */
    if (!getCommandTopics().isEmpty()) {
      // --> Commands
      KStream<String, Command> commands = builder.stream(getCommandTopics(), Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null);

      // Commands --> Results
      KStream<String, CommandResult> commandResults = commands
          .transformValues(() -> new CommandTransformer(this), "event-store", "snapshot-store")
          .filter((key, result) -> result != null);

      // Results --> Push
      commandResults
          .mapValues(CommandResult::getCommand)
          .to((key, command, recordContext) -> command.getTopicInfo().value().concat(".results"),
              Produced.with(Serdes.String(), CustomSerdes.Json(Command.class)));

      // Results --> Push to reply topic
      commandResults
          .mapValues(CommandResult::getCommand)
          .filter((key, command) -> StringUtils.isNotBlank(command.getMetadata().get(Metadata.REPLY_TO)))
          .to((key, command, recordContext) -> command.getMetadata().get(Metadata.REPLY_TO),
              Produced.with(Serdes.String(), CustomSerdes.Json(Command.class)));

      // Events --> Push
      commandResults
          .filter((key, result) -> result instanceof CommandResult.Success)
          .mapValues((key, result) -> (CommandResult.Success) result)
          .flatMapValues(CommandResult.Success::getEvents)
          .filter((key, event) -> event != null)
          .to((key, event, recordContext) -> event.getTopicInfo().value(),
              Produced.with(Serdes.String(), CustomSerdes.Json(Event.class)));
    }

    /*
     * -------------------------------------------------------------
     * EVENT HANDLING
     * -------------------------------------------------------------
     */

    if (!getEventTopics().isEmpty()) {
      // --> Events
      KStream<String, JsonNode> events = builder.stream(getEventTopics(), Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
          .filter((key, event) -> key != null)
          .filter((key, event) -> event != null);

      // Events --> Void
      events
          .transformValues(() -> new EventTransformer(this), "event-store", "snapshot-store")
          .to("upcasted-events", Produced.with(Serdes.String(), CustomSerdes.Json(Event.class)));
    }

    /*
     * -------------------------------------------------------------
     * RESULT HANDLING
     * -------------------------------------------------------------
     */

    if (!getResultTopics().isEmpty()) {
      // --> Results
      KStream<String, Command> results = builder.stream(getResultTopics(), Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null);

      // Results --> Void
      results
          .transformValues(() -> new ResultTransformer(this));
    }


    return builder.build();
  }

  public void start() {
    Topology topology = topology();
    if (topology.describe().subtopologies().isEmpty()) {
      log.info("Eventify is not started: consumer is not subscribed to any topics or assigned any partitions");
      return;
    }

    KafkaStreams kafkaStreams = new KafkaStreams(topology, getStreamsConfig());
    setUpListeners(kafkaStreams);

    log.info("Eventify is starting...");
    kafkaStreams.start();
  }

  private void setUpListeners(KafkaStreams kafkaStreams) {
    kafkaStreams.setStateListener(getStateListener());
    kafkaStreams.setUncaughtExceptionHandler(getUncaughtExceptionHandler());

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
      kafkaStreams.close(Duration.ofMillis(1000));
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


  public static class Builder {
    private final MultiValuedMap<String, Upcaster> upcasters = new ArrayListValuedHashMap<>();
    private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
    private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers = new HashMap<>();
    private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers = new ArrayListValuedHashMap<>();
    private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();

    private Properties streamsConfig;
    private StateListener stateListener;
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
    private boolean deleteEventsOnSnapshot;

    public Builder registerHandler(Object handler) {
      List<Method> upcasterMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), Upcast.class);
      List<Method> commandHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleCommand.class);
      List<Method> eventSourcingMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), ApplyEvent.class);
      List<Method> resultHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleResult.class);
      List<Method> eventHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleEvent.class);

      upcasterMethods
          .forEach(method -> addUpcaster(handler, method));

      commandHandlerMethods
          .forEach(method -> addCommandHandler(handler, method));

      eventSourcingMethods
          .forEach(method -> addEventSourcingHandler(handler, method));

      resultHandlerMethods
          .forEach(method -> addResultHandler(handler, method));

      eventHandlerMethods
          .forEach(method -> addEventHandler(handler, method));

      return this;
    }

    public Builder streamsConfig(Properties streamsConfig) {
      this.streamsConfig = streamsConfig;
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
      this.streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//
//    this.streamsConfig.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG), interceptors);

      return this;
    }

    public Builder stateListener(StateListener stateListener) {
      this.stateListener = stateListener;
      return this;
    }

    public Builder uncaughtExceptionHandler(StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
      this.uncaughtExceptionHandler = uncaughtExceptionHandler;
      return this;
    }

    public Builder deleteEventsOnSnapshot(boolean deleteEventsOnSnapshot) {
      this.deleteEventsOnSnapshot = deleteEventsOnSnapshot;
      return this;
    }

    public Eventify build() {
      return new Eventify(
          this.upcasters,
          this.commandHandlers,
          this.eventSourcingHandlers,
          this.resultHandlers,
          this.eventHandlers,
          this.streamsConfig,
          this.stateListener,
          this.uncaughtExceptionHandler,
          this.deleteEventsOnSnapshot);
    }

    private void addUpcaster(Object listener, Method method) {
      if (method.getParameterCount() == 1) {
        String type = method.getAnnotation(Upcast.class).type();
        upcasters.put(type, new Upcaster(listener, method));
      }
    }

    private void addCommandHandler(Object listener, Method method) {
      if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
        Class<?> type = method.getParameters()[0].getType();
        commandHandlers.put(type, new CommandHandler(listener, method));
      }
    }

    private void addEventSourcingHandler(Object listener, Method method) {
      if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
        Class<?> type = method.getParameters()[0].getType();
        eventSourcingHandlers.put(type, new EventSourcingHandler(listener, method));
      }
    }

    private void addResultHandler(Object listener, Method method) {
      if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
        Class<?> type = method.getParameters()[0].getType();
        resultHandlers.put(type, new ResultHandler(listener, method));
      }
    }

    private void addEventHandler(Object listener, Method method) {
      if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
        Class<?> type = method.getParameters()[0].getType();
        eventHandlers.put(type, new EventHandler(listener, method));
      }
    }

  }

}
