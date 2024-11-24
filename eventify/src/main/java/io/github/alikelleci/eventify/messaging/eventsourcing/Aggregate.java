package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.EnableSnapshotting;
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

import java.beans.Transient;
import java.time.Instant;
import java.util.Optional;

import static io.github.alikelleci.eventify.messaging.Metadata.ID;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Aggregate extends Message {
  String aggregateId;
  String eventId;
  long version;

  private Aggregate() {
    this.aggregateId = null;
    this.eventId = null;
    this.version = 0;
  }

  @Builder
  private Aggregate(Instant timestamp, Object payload, Metadata metadata, String eventId, long version) {
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

    this.eventId = eventId;
    this.version = version;

    this.metadata.add(ID, getId());
  }

  @Transient
  public int getSnapshotThreshold() {
    return Optional.ofNullable(getPayload())
        .map(Object::getClass)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, EnableSnapshotting.class))
        .map(EnableSnapshotting::threshold)
        .filter(threshold -> threshold > 0)
        .orElse(0);
  }

  @Transient
  public boolean deleteEvents() {
    return Optional.ofNullable(getPayload())
        .map(Object::getClass)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, EnableSnapshotting.class))
        .map(EnableSnapshotting::deleteEvents)
        .orElse(false);
  }
}
