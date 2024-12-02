package io.github.alikelleci.eventify.messaging.resulthandling;

import io.github.alikelleci.eventify.common.CommonParameterResolver;
import io.github.alikelleci.eventify.common.annotations.Priority;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.resulthandling.exceptions.ResultProcessingException;
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
public class ResultHandler implements Function<Command, Void>, CommonParameterResolver {

  private final Object handler;
  private final Method method;

  public ResultHandler(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
  }

  @Override
  public Void apply(Command command) {
    log.trace("Handling command result: {} ({})", command.getType(), command.getAggregateId());

    try {
      Object result = invokeHandler(handler, command);
      return null;
    } catch (Exception e) {
      throw new ResultProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object invokeHandler(Object handler, Command command) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      if (i == 0) {
        args[i] = command.getPayload();
      } else {
        args[i] = resolve(parameter, command);
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
