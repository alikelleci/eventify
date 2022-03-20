package io.github.alikelleci.eventify.experimental.messages;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;
import java.util.Optional;

@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Event extends Message {
  private String aggregateId;

  protected Event() {
    super(null, null, null, null);
  }

  protected Event(String id, Instant timestamp, Object payload, Metadata metadata) {
    super(id, timestamp, payload, metadata);
    this.aggregateId = createAggregateId(payload);
  }

  public Event(Object payload, Metadata metadata, Instant timestamp) {
    this(null, timestamp, payload, metadata);
  }

  public Event(Object payload, Metadata metadata) {
    this(null, null, payload, metadata);
  }

  public Event(Object payload) {
    this(null, null, payload, null);
  }

  private String createAggregateId(Object payload) {
    return Optional.ofNullable(payload).flatMap(p -> FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class).stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> {
          field.setAccessible(true);
          return (String) ReflectionUtils.getField(field, payload);
        }))
        .orElse(null);
  }
}
