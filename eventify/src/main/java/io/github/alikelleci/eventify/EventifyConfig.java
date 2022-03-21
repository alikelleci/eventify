package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Data
public class EventifyConfig {
  private Properties streamsConfig;
  @Setter(AccessLevel.NONE)
  private final Handlers handlers;
  @Setter(AccessLevel.NONE)
  private final Topics topics;
  private StateListener stateListener;
  private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
  private boolean deleteEventsOnSnapshot;

  protected EventifyConfig() {
    this.streamsConfig = new Properties();
    this.handlers = new Handlers();
    this.topics = new Topics(this.handlers);

    this.stateListener = (newState, oldState) ->
        log.warn("State changed from {} to {}", oldState, newState);

    this.uncaughtExceptionHandler = (throwable) ->
        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
  }


  public static class Handlers {
    private final MultiValuedMap<String, Upcaster> upcasters = new ArrayListValuedHashMap<>();
    private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
    private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers = new HashMap<>();
    private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers = new ArrayListValuedHashMap<>();
    private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();

    private Handlers() {
    }

    public MultiValuedMap<String, Upcaster> upcasters() {
      return upcasters;
    }

    public Map<Class<?>, CommandHandler> commandHandlers() {
      return commandHandlers;
    }

    public Map<Class<?>, EventSourcingHandler> eventSourcingHandlers() {
      return eventSourcingHandlers;
    }

    public MultiValuedMap<Class<?>, ResultHandler> resultHandlers() {
      return resultHandlers;
    }

    public MultiValuedMap<Class<?>, EventHandler> eventHandlers() {
      return eventHandlers;
    }
  }

  static class Topics {
    private Handlers handlers;

    private Topics(Handlers handlers) {
      this.handlers = handlers;
    }

    public Set<String> commands() {
      return handlers.commandHandlers.keySet().stream()
          .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
          .filter(Objects::nonNull)
          .map(TopicInfo::value)
          .collect(Collectors.toSet());
    }

    public Set<String> events() {
      return Stream.of(
          handlers.eventHandlers.keySet(),
          handlers.eventSourcingHandlers.keySet()
      )
          .flatMap(Collection::stream)
          .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
          .filter(Objects::nonNull)
          .map(TopicInfo::value)
          .collect(Collectors.toSet());
    }

    public Set<String> results() {
      return handlers.resultHandlers.keySet().stream()
          .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
          .filter(Objects::nonNull)
          .map(TopicInfo::value)
          .map(topic -> topic.concat(".results"))
          .collect(Collectors.toSet());
    }


  }
}
