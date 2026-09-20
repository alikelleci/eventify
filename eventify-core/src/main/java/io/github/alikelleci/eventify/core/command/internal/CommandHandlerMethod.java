package io.github.alikelleci.eventify.core.command.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.exception.CommandExecutionException;
import io.github.alikelleci.eventify.core.handler.HandlerParameterResolver;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
import io.github.alikelleci.eventify.core.message.exception.TopicMissingException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import io.github.alikelleci.eventify.core.message.internal.Topics;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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
import java.util.function.BiFunction;

@Slf4j
@Getter
public class CommandHandlerMethod implements BiFunction<AggregateState, Command, List<Object>> {

  private final Object handler;
  private final Method method;

  private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

  public CommandHandlerMethod(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
  }

  /** What the command changes about its aggregate: the payloads of the events to record, in the order returned. */
  @Override
  public List<Object> apply(AggregateState state, Command command) {
    try {
      validate(command.getPayload());
      Object result = invokeHandler(handler, state, command);
      return eventPayloads(command, result);
    } catch (Exception e) {
      throw new CommandExecutionException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object invokeHandler(Object handler, AggregateState state, Command command) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      if (i == 0) {
        args[i] = command.getPayload();
      } else if (parameter.getType().isAnnotationPresent(AggregateRoot.class)) {
        args[i] = state != null ? state.getPayload() : null;
      } else {
        args[i] = HandlerParameterResolver.resolve(parameter, command);
      }
    }

    // Invoke the method
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
      // The events of a command belong to the aggregate the command was handled for: another id would be stored and
      // replayed as another aggregate's event.
      String aggregateId = AggregateIdResolver.getAggregateId(payload);
      if (!StringUtils.equals(aggregateId, command.getAggregateId())) {
        throw new AggregateIdMismatchException("Aggregate identifier does not match for event " + type + ". Expected " + command.getAggregateId() + ", but was " + aggregateId);
      }
      // The topic an event is sent to is only looked up when it is sent, after it is stored: an event without one
      // is rejected here, before anything of the command is stored.
      if (Topics.of(payload.getClass()) == null) {
        throw new TopicMissingException("Event " + type + " has no topic. Please annotate its class, or an interface it implements, with @Topic.");
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
