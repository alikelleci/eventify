package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.EnableSnapshotting;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.eventify.messaging.Message;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.ToString;
import lombok.Value;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.github.alikelleci.eventify.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.RESULT;

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
  private Aggregate(Instant timestamp, Object payload, @Singular("metadata") Map<String, String> metadata, String eventId, long version) {
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

    this.metadata = extendMetadata(metadata);
  }

  private Map<String, String> extendMetadata(Map<String, String> metadata) {
    Map<String, String> map = new HashMap<>(MapUtils.emptyIfNull(metadata));
    map.put(ID, getId());
    map.remove(RESULT);
    map.remove(CAUSE);

    return new HashMap<>(map);
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
