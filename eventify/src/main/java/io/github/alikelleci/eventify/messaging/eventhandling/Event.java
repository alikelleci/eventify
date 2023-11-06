package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.Revision;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;
import java.util.Optional;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Event extends Message {
  String aggregateId;
  int revision;

  private Event() {
    this.aggregateId = null;
    this.revision = 1;
  }

  @Builder
  private Event(Instant timestamp, Object payload, Metadata metadata) {
    super(timestamp, payload, metadata);

    this.aggregateId = Optional.ofNullable(payload)
        .flatMap(p -> FieldUtils.getFieldsListWithAnnotation(p.getClass(), AggregateId.class).stream()
            .filter(field -> field.getType() == String.class)
            .findFirst()
            .map(field -> {
              field.setAccessible(true);
              return (String) ReflectionUtils.getField(field, p);
            }))
        .orElse(null);

    this.revision = Optional.ofNullable(payload)
        .map(Object::getClass)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, Revision.class))
        .map(Revision::value)
        .orElse(1);
  }
}
