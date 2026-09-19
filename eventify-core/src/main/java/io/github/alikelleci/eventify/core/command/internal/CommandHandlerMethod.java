package io.github.alikelleci.eventify.core.command.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.exception.CommandExecutionException;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.handler.HandlerParameterResolver;
import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
import io.github.alikelleci.eventify.core.message.exception.TopicMissingException;
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

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;

@Slf4j
@Getter
public class CommandHandlerMethod implements BiFunction<AggregateState, Command, List<Event>> {

  private final Object handler;
  private final Method method;

  private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

  public CommandHandlerMethod(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
  }

  @Override
  public List<Event> apply(AggregateState state, Command command) {
    try {
      validate(command.getPayload());
      Object result = invokeHandler(handler, state, command);
      return createEvents(command, result);
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

  private List<Event> createEvents(Command command, Object result) {
    if (result == null) {
      return new ArrayList<>();
    }

    List<Object> list = new ArrayList<>();
    if (List.class.isAssignableFrom(result.getClass())) {
      list.addAll((List<?>) result);
    } else {
      list.add(result);
    }

    List<Event> events = list.stream()
        .filter(Objects::nonNull)
        .map(payload -> Event.builder()
            .timestamp(command.getTimestamp())
            .payload(payload)
            .metadata(command.getMetadata())
            .metadata(CAUSATION_ID, command.getId())
            .build())
        .toList();

    events.forEach(event -> {
      if (!StringUtils.equals(event.getAggregateId(), command.getAggregateId())) {
        throw new AggregateIdMismatchException("Aggregate identifier does not match for event " + event.getType() + ". Expected " + command.getAggregateId() + ", but was " + event.getAggregateId());
      }
      // The topic an event is sent to is only looked up when it is sent, after it is stored: an event without one
      // is rejected here, before anything of the command is stored.
      if (AnnotationScanner.findAnnotation(event.getPayload().getClass(), Topic.class) == null) {
        throw new TopicMissingException("Event " + event.getType() + " has no topic. Please annotate its class, or an interface it implements, with @Topic.");
      }
    });

    return events;
  }

  private void validate(Object payload) {
    Set<ConstraintViolation<Object>> violations = VALIDATOR.validate(payload);
    if (!CollectionUtils.isEmpty(violations)) {
      throw new ConstraintViolationException(violations);
    }
  }

}
