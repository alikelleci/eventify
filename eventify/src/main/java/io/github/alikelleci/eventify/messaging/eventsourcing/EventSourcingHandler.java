package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.common.CommonParameterResolver;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.exceptions.AggregateInvocationException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.function.BiFunction;

@Slf4j
@Getter
public class EventSourcingHandler implements BiFunction<AggregateState, Event, AggregateState>, CommonParameterResolver {

  private final Object handler;
  private final Method method;

  public EventSourcingHandler(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
  }

  @Override
  public AggregateState apply(AggregateState state, Event event) {
    log.trace("Applying event: {} ({})", event.getType(), event.getAggregateId());

    try {
      Object result = invokeHandler(handler, state, event);
      return createState(event, result);
    } catch (Exception e) {
      throw new AggregateInvocationException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object invokeHandler(Object handler, AggregateState state, Event event) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      if (i == 0) {
        args[i] = state != null ? state.getPayload() : null;
      } else if (i == 1) {
        args[i] = event.getPayload();
      } else {
        args[i] = resolve(parameter, event);
      }
    }

    // Invoke the method
    return method.invoke(handler, args);
  }

  private AggregateState createState(Event event, Object result) {
    if (result == null) {
      return null;
    }

    return AggregateState.builder()
        .timestamp(event.getTimestamp())
        .payload(result)
        .metadata(event.getMetadata())
        .eventId(event.getId())
        .build();
  }

}
