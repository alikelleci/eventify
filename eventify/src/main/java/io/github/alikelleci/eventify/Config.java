package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Config {

  private final Handlers handlers;
  private final Topics topics;

  private StateListener stateListener = (newState, oldState) ->
      log.warn("State changed from {} to {}", oldState, newState);

  private StreamsUncaughtExceptionHandler uncaughtExceptionHandler = (throwable) ->
      StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

  private boolean deleteEventsOnSnapshot;

  protected Config() {
    this.handlers = new Handlers();
    this.topics = new Topics(this.handlers);
  }

  public Handlers handlers() {
    return handlers;
  }

  public Topics topics() {
    return topics;
  }

  public void setStateListener(StateListener stateListener) {
    this.stateListener = stateListener;
  }

  public void setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
  }

  public void setDeleteEventsOnSnapshot(boolean deleteEventsOnSnapshot) {
    this.deleteEventsOnSnapshot = deleteEventsOnSnapshot;
  }

  public StateListener stateListener() {
    return stateListener;
  }

  public StreamsUncaughtExceptionHandler uncaughtExceptionHandler() {
    return uncaughtExceptionHandler;
  }

  public boolean isDeleteEventsOnSnapshot() {
    return deleteEventsOnSnapshot;
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
