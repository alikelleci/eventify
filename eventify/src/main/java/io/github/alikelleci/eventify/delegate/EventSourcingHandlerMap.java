package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.experimental.Delegate;

import java.lang.reflect.Method;
import java.util.Map;

public class EventSourcingHandlerMap implements Map<Class<?>, EventSourcingHandler> {

  @Delegate
  private Map<Class<?>, EventSourcingHandler> map;

  public void putHandler(Object listener) {
    HandlerUtils.findMethodsWithAnnotation(listener.getClass(), ApplyEvent.class)
        .forEach(method -> add(listener, method));
  }

  private void add(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      this.put(type, new EventSourcingHandler(listener, method));
    }
  }

}
