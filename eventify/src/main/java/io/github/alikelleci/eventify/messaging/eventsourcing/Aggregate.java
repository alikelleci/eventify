package io.github.alikelleci.eventify.messaging.eventsourcing;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.alikelleci.eventify.common.annotations.EnableSnapshots;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import org.springframework.core.annotation.AnnotationUtils;

import java.time.Instant;
import java.util.Optional;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Aggregate extends Message {
  private String aggregateId;
  private String eventId;
  private long version;

  @Builder
  protected Aggregate(Instant timestamp, Object payload, Metadata metadata, String aggregateId, String eventId, long version) {
    super(timestamp, payload, metadata);

    this.aggregateId = aggregateId;
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


}
