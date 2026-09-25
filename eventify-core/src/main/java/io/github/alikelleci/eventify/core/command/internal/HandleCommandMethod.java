package io.github.alikelleci.eventify.core.command.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.exception.CommandExecutionException;
import io.github.alikelleci.eventify.core.handler.HandlerParameterResolver;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
import io.github.alikelleci.eventify.core.message.exception.TopicMissingException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import io.github.alikelleci.eventify.core.message.internal.Topics;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Getter
public class HandleCommandMethod {

  private final Object handler;
  private final Method method;
  /** The {@code @AggregateRoot} name of the aggregate this handler works on. */
  private final String aggregateType;

  private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

  public HandleCommandMethod(Object handler, Method method, String aggregateType) {
    this.handler = handler;
    this.method = method;
    this.aggregateType = aggregateType;
  }

  /** The event payloads the command produces, in order; the processor turns them into events. */
  public List<Object> handle(Command command, AggregateState state) {
    try {
      validate(command.getPayload());
      Object result = invokeHandler(command, state);
      return eventPayloads(command, result);
    } catch (Exception e) {
      throw new CommandExecutionException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object invokeHandler(Command command, AggregateState state) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      if (i == 0) {
        args[i] = command.getPayload();
      } else if (parameter.getType().isAnnotationPresent(AggregateRoot.class)) {
        args[i] = state.getPayload();
      } else {
        args[i] = HandlerParameterResolver.resolve(parameter, command);
      }
    }

    return method.invoke(handler, args);
  }

  private List<Object> eventPayloads(Command command, Object result) {
    List<Object> payloads = new ArrayList<>();
    if (result instanceof List<?> list) {
      list.stream().filter(Objects::nonNull).forEach(payloads::add);
    } else if (result != null) {
      payloads.add(result);
    }

    payloads.forEach(payload -> {
      String type = payload.getClass().getSimpleName();
      // Another id would be stored and replayed as another aggregate's event.
      String aggregateId = AggregateIdResolver.getAggregateId(payload);
      if (!StringUtils.equals(aggregateId, command.getAggregateId())) {
        throw new AggregateIdMismatchException("Event " + type + " has aggregate id " + aggregateId + ", expected " + command.getAggregateId());
      }
      // Checked now: the topic is only looked up when sending, after the events are stored.
      if (Topics.of(payload.getClass()) == null) {
        throw new TopicMissingException("Event " + type + " has no @Topic.");
      }
    });

    return payloads;
  }

  private void validate(Object payload) {
    Set<ConstraintViolation<Object>> violations = VALIDATOR.validate(payload);
    if (!CollectionUtils.isEmpty(violations)) {
      throw new ConstraintViolationException(violations);
    }
  }

}
