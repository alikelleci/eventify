package io.github.alikelleci.eventify.core.event.internal;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.annotation.Priority;
import io.github.alikelleci.eventify.core.event.exception.EventHandlingException;
import io.github.alikelleci.eventify.core.handler.HandlerParameterResolver;
import io.github.alikelleci.eventify.core.message.exception.TopicMissingException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import io.github.alikelleci.eventify.core.message.internal.Topics;
import lombok.Getter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Getter
public class EventHandlerMethod {

  private final Object handler;
  private final Method method;
  private final int priority;

  public EventHandlerMethod(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
    this.priority = Optional.ofNullable(method.getAnnotation(Priority.class))
        .map(Priority::value)
        .orElse(0);
  }

  /** The command payloads the handler returns, in order; empty for a void handler. */
  public List<Object> handle(Event event) {
    try {
      Object result = invokeHandler(event);
      return commandPayloads(result);
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

  private List<Object> commandPayloads(Object result) {
    List<Object> payloads = new ArrayList<>();
    if (result instanceof List<?> list) {
      list.stream().filter(Objects::nonNull).forEach(payloads::add);
    } else if (result != null) {
      payloads.add(result);
    }

    payloads.forEach(payload -> {
      // Checked now: the topic and key are only looked up when sending.
      if (Topics.of(payload.getClass()) == null) {
        throw new TopicMissingException("Command " + payload.getClass().getSimpleName() + " has no @Topic.");
      }
      AggregateIdResolver.getAggregateId(payload);
    });

    return payloads;
  }
}
