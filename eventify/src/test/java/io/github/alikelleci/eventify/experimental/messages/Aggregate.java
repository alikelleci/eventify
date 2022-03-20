package io.github.alikelleci.eventify.experimental.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.EnableSnapshots;
import io.github.alikelleci.eventify.messaging.Metadata;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;
import java.util.Optional;

@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Aggregate extends Message {
  private String aggregateId;
  private String eventId;
  private long version;

  protected Aggregate() {
    super(null, null, null, null);
  }

  protected Aggregate(String id, Instant timestamp, Object payload, Metadata metadata, String eventId, long version) {
    super(id, timestamp, payload, metadata);
    this.aggregateId = createAggregateId(payload);
    this.eventId = eventId;
    this.version = version;
  }

  @JsonIgnore
  public int getSnapshotTreshold() {
    return Optional.ofNullable(getPayload())
        .map(Object::getClass)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, EnableSnapshots.class))
        .map(EnableSnapshots::threshold)
        .filter(threshold -> threshold > 0)
        .orElse(0);
  }

  public static AggregateBuilder builder() {
    return new AggregateBuilder();
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

  public static class AggregateBuilder {
    private Object payload;
    private Metadata metadata;
    private Instant timestamp;
    private String eventId;
    private long version;

    public AggregateBuilder payload(Object payload) {
      this.payload = payload;
      return this;
    }

    public AggregateBuilder metadata(Metadata metadata) {
      this.metadata = metadata;
      return this;
    }

    public AggregateBuilder timestamp(Instant timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public AggregateBuilder eventId(String eventId) {
      this.eventId = eventId;
      return this;
    }

    public AggregateBuilder version(long version) {
      this.version = version;
      return this;
    }

    public Aggregate build() {
      return new Aggregate(null, this.timestamp, this.payload, this.metadata, eventId, version);
    }
  }

}
