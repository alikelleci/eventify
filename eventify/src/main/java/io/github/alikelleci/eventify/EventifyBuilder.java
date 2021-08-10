package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.constants.Handlers;
import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class EventifyBuilder {

  private final Properties streamsConfig;

  private KafkaStreams.StateListener stateListener;
  private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

  public EventifyBuilder(Properties streamsConfig) {
    this.streamsConfig = streamsConfig;
    this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    this.streamsConfig.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    this.streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
    this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//
//    this.streamsConfig.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG), interceptors);
  }

  public EventifyBuilder setStateListener(KafkaStreams.StateListener stateListener) {
    this.stateListener = stateListener;
    return this;
  }

  public EventifyBuilder setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
    this.uncaughtExceptionHandler = exceptionHandler;
    return this;
  }

  public EventifyBuilder registerHandler(Object handler) {
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

  public Eventify build() {
    createTopics();
    return new Eventify(
        streamsConfig,
        stateListener,
        uncaughtExceptionHandler);
  }

  private void addUpcaster(Object listener, Method method) {
    if (method.getParameterCount() == 1) {
      String type = method.getAnnotation(Upcast.class).type();
      Handlers.UPCASTERS.put(type, new Upcaster(listener, method));
    }
  }

  private void addCommandHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      Handlers.COMMAND_HANDLERS.put(type, new CommandHandler(listener, method));

      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        Topics.COMMANDS.add(topicInfo.value());
      }
    }
  }

  private void addEventSourcingHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      Handlers.EVENTSOURCING_HANDLERS.put(type, new EventSourcingHandler(listener, method));

      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        Topics.EVENTS.add(topicInfo.value());
      }
    }
  }

  private void addResultHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      Handlers.RESULT_HANDLERS.put(type, new ResultHandler(listener, method));

      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        Topics.RESULTS.add(topicInfo.value().concat(".results"));
      }
    }
  }

  private void addEventHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      Handlers.EVENT_HANDLERS.put(type, new EventHandler(listener, method));

      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        Topics.EVENTS.add(topicInfo.value());
      }
    }
  }

  private void createTopics() {
    Properties properties = new Properties();

    String boostrapServers = streamsConfig.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    if (StringUtils.isNotBlank(boostrapServers)) {
      properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
    }

    String securityProtocol = streamsConfig.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    if (StringUtils.isNotBlank(securityProtocol)) {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    }

    try (AdminClient adminClient = AdminClient.create(properties)) {
      ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
      listTopicsOptions.timeoutMs(15000);
      Set<String> brokerTopics = adminClient.listTopics(listTopicsOptions).names().get();

      Set<NewTopic> topicsToCreate = Stream.of(
          Topics.COMMANDS,
          Topics.EVENTS,
          Topics.RESULTS
      )
          .flatMap(Collection::stream)
          .filter(topic -> !brokerTopics.contains(topic))
          .map(topic -> new NewTopic(topic, 1, (short) 1))
          .collect(Collectors.toSet());

      CreateTopicsOptions options = new CreateTopicsOptions();
      options.timeoutMs(15000);
      adminClient.createTopics(topicsToCreate, options).all().get();

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }
}
