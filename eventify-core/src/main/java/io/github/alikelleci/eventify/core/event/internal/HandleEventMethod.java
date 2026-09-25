package io.github.alikelleci.eventify.core.event.internal;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.annotation.Priority;
import io.github.alikelleci.eventify.core.event.exception.EventHandlingException;
import io.github.alikelleci.eventify.core.handler.HandlerParameterResolver;
import lombok.Getter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Optional;

@Getter
public class HandleEventMethod {

  private final Object handler;
  private final Method method;
  private final int priority;

  public HandleEventMethod(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
    this.priority = Optional.ofNullable(method.getAnnotation(Priority.class))
        .map(Priority::value)
        .orElse(0);
  }

  public void handle(Event event) {
    try {
      invokeHandler(event);
    } catch (Exception e) {
      throw new EventHandlingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object invokeHandler(Event event) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      if (i == 0) {
        args[i] = event.getPayload();
      } else {
        args[i] = HandlerParameterResolver.resolve(parameter, event);
      }
    }

    return method.invoke(handler, args);
  }
}
