package io.github.alikelleci.eventify.core.aggregate.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.exception.EventSourcingException;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.handler.HandlerParameterResolver;
import io.github.alikelleci.eventify.core.internal.ExceptionCauses;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

@Slf4j
@Getter
public class ApplyEventMethod {

  private final Object handler;
  private final Method method;

  public ApplyEventMethod(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
  }

  /**
   * Applies the event: the aggregate after it, {@code null} when the event removes it. The aggregate repository makes
   * it a state: it knows which aggregate is rebuilt, and the revision of its class.
   */
  public Object apply(Event event, AggregateState state) {
    try {
      Object result = invokeHandler(event, state);
      requireSameAggregateId(event, result);
      return result;
    } catch (Exception e) {
      throw new EventSourcingException(ExceptionUtils.getRootCauseMessage(e), ExceptionCauses.rootCauseOrSelf(e));
    }
  }

  private Object invokeHandler(Event event, AggregateState state) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      if (i == 0) {
        args[i] = event.getPayload();
      } else if (parameter.getType().isAnnotationPresent(AggregateRoot.class)) {
        args[i] = state.getPayload();
      } else {
        args[i] = HandlerParameterResolver.resolve(parameter, event);
      }
    }

    // Invoke the method
    return method.invoke(handler, args);
  }

  private void requireSameAggregateId(Event event, Object result) {
    if (result != null) {
      String stateAggregateId = AggregateIdResolver.getAggregateId(result);
      // A state with another id would otherwise be stored under this event's aggregate id as a snapshot.
      if (!StringUtils.equals(stateAggregateId, event.getAggregateId())) {
        throw new AggregateIdMismatchException("Aggregate identifier does not match for state " + result.getClass().getSimpleName() + " after event " + event.getType() + ". Expected " + event.getAggregateId() + ", but was " + stateAggregateId);
      }
    }
  }

}
