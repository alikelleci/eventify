package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.Revision;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
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

  @Builder(toBuilder = true)
  private Event(Instant timestamp, Object payload, Metadata metadata) {
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

    this.revision = Optional.ofNullable(getPayload())
        .map(Object::getClass)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, Revision.class))
        .map(Revision::value)
        .orElse(1);
  }

  public static class EventBuilder {
    private final Metadata.MetadataBuilder metadataBuilder = Metadata.builder();

    public EventBuilder metadata(String key, String value) {
      this.metadataBuilder.entry(key, value);
      return this;
    }

    public EventBuilder metadata(Metadata metadata) {
      if (metadata != null) {
        metadata.getEntries().forEach(metadataBuilder::entry);
      }
      return this;
    }

    public Event build() {
      Metadata metadata = metadataBuilder.build();
      return new Event(timestamp, payload, metadata);
    }
  }
}
