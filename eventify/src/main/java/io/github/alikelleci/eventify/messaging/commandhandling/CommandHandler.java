package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.common.exceptions.AggregateIdMismatchException;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.common.exceptions.TopicInfoMissingException;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.ValidationException;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Slf4j
public class CommandHandler implements BiFunction<Aggregate, Command, List<Event>> {

  private final Object target;
  private final Method method;

  private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

  public CommandHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
 }

  @Override
  public List<Event> apply(Aggregate aggregate, Command command) {
    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());
    try {
      validate(command.getPayload());
      return doInvoke(aggregate, command);

    } catch (Exception e) {
      Throwable throwable = ExceptionUtils.getRootCause(e);
      String message = ExceptionUtils.getRootCauseMessage(e);
      if (throwable instanceof ValidationException) {
        log.debug("Handling command failed: ", throwable);
      } else {
        log.error("Handling command failed: ", throwable);
      }
      throw new CommandExecutionException(message, throwable);
    }
  }

  private List<Event> doInvoke(Aggregate aggregate, Command command) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, command.getPayload());
    } else {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, command.getPayload(), command.getMetadata());
    }
    return createEvents(command, result);
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
            .metadata(Metadata.builder()
                .addAll(command.getMetadata())
                .build())
            .build())
        .collect(Collectors.toList());

    events.forEach(event -> {
      if (event.getPayload() == null) {
        throw new PayloadMissingException("You are trying to publish an event without a payload.");
      }
      if (event.getTopicInfo() == null) {
        throw new TopicInfoMissingException("You are trying to publish an event without any topic information. Please annotate your event with @TopicInfo.");
      }
      if (event.getAggregateId() == null) {
        throw new AggregateIdMissingException("You are trying to publish an event without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
      }
      if (!StringUtils.equals(event.getAggregateId(), command.getAggregateId())) {
        throw new AggregateIdMismatchException("Aggregate identifier does not match. Expected " + command.getAggregateId() + ", but was " + event.getAggregateId());
      }
    });

    return events;
  }

  private void validate(Object payload) {
    Set<ConstraintViolation<Object>> violations = validator.validate(payload);
    if (!CollectionUtils.isEmpty(violations)) {
      throw new ConstraintViolationException(violations);
    }
  }

  public Method getMethod() {
    return method;
  }
}
