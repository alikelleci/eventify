package io.github.alikelleci.eventify.messaging.resulthandling;

import io.github.alikelleci.eventify.common.annotations.MessageId;
import io.github.alikelleci.eventify.common.annotations.MetadataValue;
import io.github.alikelleci.eventify.common.annotations.Priority;
import io.github.alikelleci.eventify.common.annotations.Timestamp;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.resulthandling.exceptions.ResultProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
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
    log.trace("Handling command result: {} ({})", command.getType(), command.getAggregateId());

    try {
      Object result = invokeHandler(target, command);
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
        continue;
      }

      if (parameter.getType().isAssignableFrom(Metadata.class)) {
        args[i] = command.getMetadata();
      } else if (parameter.isAnnotationPresent(Timestamp.class)) {
        args[i] = command.getTimestamp();
      } else if (parameter.isAnnotationPresent(MessageId.class)) {
        args[i] = command.getId();
      } else if (parameter.isAnnotationPresent(MetadataValue.class)) {
        MetadataValue annotation = parameter.getAnnotation(MetadataValue.class);
        String key = annotation.value();
        args[i] = key.isEmpty() ? command.getMetadata() : command.getMetadata().get(key);
      } else {
        throw new IllegalArgumentException("Unsupported parameter: " + parameter);
      }
    }

    // Invoke the method
    return method.invoke(handler, args);
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
