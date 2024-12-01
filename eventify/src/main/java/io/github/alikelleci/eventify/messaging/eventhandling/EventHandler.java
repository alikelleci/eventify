package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.common.annotations.Priority;
import io.github.alikelleci.eventify.messaging.Context;
import io.github.alikelleci.eventify.messaging.eventhandling.exceptions.EventProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
public class EventHandler implements Function<Event, Void> {

  private final Object target;
  private final Method method;

  public EventHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
  }

  @Override
  public Void apply(Event event) {
    log.trace("Handling event: {} ({})", event.getType(), event.getAggregateId());

    try {
      return doInvoke(event);
    } catch (Exception e) {
      throw new EventProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Void doInvoke(Event event) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 1) {
      result = method.invoke(target, event.getPayload());
    } else {
      boolean injectContext = method.getParameters()[1].getType() == Context.class;
      result = method.invoke(target, event.getPayload(), injectContext ? new Context(event) : event.getMetadata());
    }
    return null;
  }

  public Method getMethod() {
    return method;
  }

  public int getPriority() {
    return Optional.ofNullable(method.getAnnotation(Priority.class))
        .map(Priority::value)
        .orElse(0);
  }
}
