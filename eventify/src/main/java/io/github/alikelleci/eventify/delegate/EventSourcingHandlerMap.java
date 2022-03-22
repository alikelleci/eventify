package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.experimental.Delegate;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class EventSourcingHandlerMap implements Map<Class<?>, EventSourcingHandler> {

  @Delegate(excludes = ExcludedMethods.class)
  private Map<Class<?>, EventSourcingHandler> map;

  @Override
  public EventSourcingHandler put(Class<?> type, EventSourcingHandler handler) {
    List<Method> eventHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), ApplyEvent.class);
    eventHandlerMethods
        .forEach(method -> addEventSourcingHandler(handler, method));

    return handler;
  }

  private void addEventSourcingHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      this.put(type, new EventSourcingHandler(listener, method));
    }
  }


  /**
   * Excluded methods that Lombok will not implement, we will implement/override these methods.
   */
  private abstract class ExcludedMethods {

    public abstract boolean put(Class<?> type, EventSourcingHandler handler);
  }
}
