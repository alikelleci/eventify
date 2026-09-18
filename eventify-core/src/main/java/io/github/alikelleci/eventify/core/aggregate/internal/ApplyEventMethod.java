package io.github.alikelleci.eventify.core.aggregate.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.exception.EventSourcingException;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.handler.internal.HandlerParameterResolver;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.function.BiFunction;

@Slf4j
@Getter
public class ApplyEventMethod implements BiFunction<AggregateState, Event, AggregateState>, HandlerParameterResolver {

  private final Object handler;
  private final Method method;

  public ApplyEventMethod(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
  }

  @Override
  public AggregateState apply(AggregateState state, Event event) {
    try {
      Object result = invokeHandler(handler, state, event);
      return createState(event, result);
    } catch (Exception e) {
      throw new EventSourcingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object invokeHandler(Object handler, AggregateState state, Event event) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      if (i == 0) {
        args[i] = event.getPayload();
      } else if (parameter.getType().isAnnotationPresent(AggregateRoot.class)) {
        args[i] = state != null ? state.getPayload() : null;
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

    AggregateState state = AggregateState.builder()
        .timestamp(event.getTimestamp())
        .payload(result)
        .metadata(event.getMetadata())
        .eventId(event.getId())
        .build();

    // The state is stored as the snapshot of its own aggregate id: another id would overwrite that aggregate's snapshot.
    if (!StringUtils.equals(state.getAggregateId(), event.getAggregateId())) {
      throw new AggregateIdMismatchException("Aggregate identifier does not match for state " + state.getType() + " after event " + event.getType() + ". Expected " + event.getAggregateId() + ", but was " + state.getAggregateId());
    }
    return state;
  }

}
