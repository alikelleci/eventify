package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.util.HandlerUtils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventSourcingHandlers {
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers = new HashMap<>();

  public void registerHandler(Object handler) {
    List<Method> eventSourcingMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), ApplyEvent.class);

    eventSourcingMethods
        .forEach(method -> addEventSourcingHandler(handler, method));
  }

  private void addEventSourcingHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      eventSourcingHandlers.put(type, new EventSourcingHandler(listener, method));
    }
  }

}
