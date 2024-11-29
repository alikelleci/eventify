package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Command extends Message {
  String aggregateId;

  private Command() {
    this.aggregateId = null;
  }

  @Builder
  private Command(Instant timestamp, Object payload, Metadata metadata) {
    super(timestamp, payload, metadata);

    this.aggregateId = FieldUtils.getFieldsListWithAnnotation(getPayload().getClass(), AggregateId.class)
        .stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> {
          field.setAccessible(true);
          return (String) ReflectionUtils.getField(field, getPayload());
        })
        .orElseThrow(() -> new AggregateIdMissingException("Aggregate identifier missing. Please annotate your field containing the identifier with @AggregateId."));

    this.id = this.aggregateId + "@" + getId();
  }
}
