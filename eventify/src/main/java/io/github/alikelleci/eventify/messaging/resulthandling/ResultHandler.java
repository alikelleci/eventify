package io.github.alikelleci.eventify.messaging.resulthandling;

import io.github.alikelleci.eventify.common.annotations.Priority;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.resulthandling.exceptions.ResultProcessingException;
import io.github.alikelleci.eventify.retry.Retry;
import io.github.alikelleci.eventify.retry.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
public class ResultHandler implements Function<Command, Void> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  public ResultHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Handling command result failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Handling command result failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public Void apply(Command command) {
    log.debug("Handling command result: {} ({})", command.getPayload().getClass().getSimpleName(), command.getAggregateId());

    try {
      return Failsafe.with(retryPolicy).get(() -> doInvoke(command));
    } catch (Exception e) {
      throw new ResultProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Void doInvoke(Command command) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 1) {
      result = method.invoke(target, command.getPayload());
    } else {
      result = method.invoke(target, command.getPayload(), command.getMetadata());
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
