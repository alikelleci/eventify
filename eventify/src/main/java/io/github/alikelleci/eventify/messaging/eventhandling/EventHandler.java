package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.common.CommonParameterResolver;
import io.github.alikelleci.eventify.common.annotations.Priority;
import io.github.alikelleci.eventify.messaging.eventhandling.exceptions.EventProcessingException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
@Getter
public class EventHandler implements Function<Event, Void>, CommonParameterResolver {

  private final Object handler;
  private final Method method;

  public EventHandler(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
  }

  @Override
  public Void apply(Event event) {
    log.trace("Handling event: {} ({})", event.getType(), event.getAggregateId());

    try {
      Object result = invokeHandler(handler, event);
      return null;
    } catch (Exception e) {
      throw new EventProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object invokeHandler(Object handler, Event event) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      if (i == 0) {
        args[i] = event.getPayload();
      } else {
        args[i] = resolve(parameter, event);
      }
    }

    // Invoke the method
    return method.invoke(handler, args);
  }

  public int getPriority() {
    return Optional.ofNullable(method.getAnnotation(Priority.class))
        .map(Priority::value)
        .orElse(0);
  }
}
