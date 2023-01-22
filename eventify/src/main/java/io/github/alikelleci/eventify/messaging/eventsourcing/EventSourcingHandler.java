package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.exceptions.AggregateInvocationException;
import io.github.alikelleci.eventify.retry.Retry;
import io.github.alikelleci.eventify.retry.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.BiFunction;

@Slf4j
public class EventSourcingHandler implements BiFunction<Aggregate, Event, Aggregate> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  public EventSourcingHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Applying event failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Applying event failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public Aggregate apply(Aggregate aggregate, Event event) {
    log.debug("Applying event: {} ({})", event.getType(), event.getAggregateId());

    try {
      return Failsafe.with(retryPolicy).get(() -> doInvoke(aggregate, event));
    } catch (Exception e) {
      throw new AggregateInvocationException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Aggregate doInvoke(Aggregate aggregate, Event event) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload());
    } else {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload(), event.getMetadata());
    }
    return createState(event, result);
  }

  private Aggregate createState(Event event, Object result) {
    if (result == null) {
      return null;
    }

    return Aggregate.builder()
        .timestamp(event.getTimestamp())
        .payload(result)
        .metadata(Metadata.builder()
            .addAll(event.getMetadata())
            .build())
        .eventId(event.getId())
        .build();
  }

  public Method getMethod() {
    return method;
  }
}
