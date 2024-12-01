package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.common.annotations.MessageId;
import io.github.alikelleci.eventify.common.annotations.MetadataValue;
import io.github.alikelleci.eventify.common.annotations.Timestamp;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.exceptions.AggregateInvocationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.function.BiFunction;

@Slf4j
public class EventSourcingHandler implements BiFunction<Aggregate, Event, Aggregate> {

  private final Object target;
  private final Method method;

  public EventSourcingHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
  }

  @Override
  public Aggregate apply(Aggregate aggregate, Event event) {
    log.trace("Applying event: {} ({})", event.getType(), event.getAggregateId());

    try {
      Object result = invokeHandler(target, aggregate, event);
      return createState(event, result);
    } catch (Exception e) {
      throw new AggregateInvocationException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object invokeHandler(Object handler, Aggregate aggregate, Event event) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];

      if (i == 0) {
        args[i] = aggregate != null ? aggregate.getPayload() : null;
        continue;
      }

      if (i == 1) {
        args[i] = event.getPayload();
        continue;
      }

      if (parameter.getType().isAssignableFrom(Metadata.class)) {
        args[i] = event.getMetadata();
      } else if (parameter.isAnnotationPresent(Timestamp.class)) {
        args[i] = event.getTimestamp();
      } else if (parameter.isAnnotationPresent(MessageId.class)) {
        args[i] = event.getId();
      } else if (parameter.isAnnotationPresent(MetadataValue.class)) {
        MetadataValue annotation = parameter.getAnnotation(MetadataValue.class);
        String key = annotation.value();
        args[i] = key.isEmpty() ? event.getMetadata() : event.getMetadata().get(key);
      } else {
        throw new IllegalArgumentException("Unsupported parameter: " + parameter);
      }
    }

    // Invoke the method
    return method.invoke(handler, args);
  }

  private Aggregate createState(Event event, Object result) {
    if (result == null) {
      return null;
    }

    return Aggregate.builder()
        .timestamp(event.getTimestamp())
        .payload(result)
        .metadata(event.getMetadata())
        .eventId(event.getId())
        .build();
  }

  public Method getMethod() {
    return method;
  }

}
