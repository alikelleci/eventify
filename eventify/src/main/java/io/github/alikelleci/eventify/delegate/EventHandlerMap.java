package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.experimental.Delegate;
import org.apache.commons.collections4.MultiValuedMap;

import java.lang.reflect.Method;

public class EventHandlerMap implements MultiValuedMap<Class<?>, EventHandler> {

  @Delegate
  private MultiValuedMap<Class<?>, EventHandler> map;

  public void putHandler(Class<?> type, EventHandler handler) {
    HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleEvent.class)
        .forEach(method -> add(handler, method));
  }

  private void add(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      this.put(type, new EventHandler(listener, method));
    }
  }

}
