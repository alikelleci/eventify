package io.github.alikelleci.eventify.messaging.resulthandling;

import io.github.alikelleci.eventify.common.annotations.Priority;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.resulthandling.exceptions.ResultProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
public class ResultHandler implements Function<Command, Void> {

  private final Object target;
  private final Method method;

  public ResultHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
 }

  @Override
  public Void apply(Command command) {
    log.debug("Handling command result: {} ({})", command.getType(), command.getAggregateId());

    try {
      return doInvoke(command);
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
