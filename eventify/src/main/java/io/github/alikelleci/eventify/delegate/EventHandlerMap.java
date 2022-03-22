package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.experimental.Delegate;
import org.apache.commons.collections4.MultiValuedMap;

import java.lang.reflect.Method;
import java.util.List;

public class EventHandlerMap implements MultiValuedMap<Class<?>, EventHandler> {

  @Delegate(excludes = ExcludedMethods.class)
  private MultiValuedMap<Class<?>, EventHandler> map;

  @Override
  public boolean put(Class<?> type, EventHandler handler) {
    List<Method> eventHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleEvent.class);
    eventHandlerMethods
        .forEach(method -> addEventHandler(handler, method));

    return true;
  }

  private void addEventHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      this.put(type, new EventHandler(listener, method));
    }
  }

  /**
   * Excluded methods that Lombok will not implement, we will implement/override these methods.
   */
  private abstract class ExcludedMethods {

    public abstract boolean put(Class<?> type, EventHandler handler);
  }
}
