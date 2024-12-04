package io.github.alikelleci.eventify.core.messaging.eventsourcing;

import io.github.alikelleci.eventify.core.common.annotations.EnableSnapshotting;
import io.github.alikelleci.eventify.core.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.core.messaging.Message;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.util.AnnotationScanner;
import io.github.alikelleci.eventify.core.util.IdUtils;
import lombok.Builder;
import lombok.Value;

import java.beans.Transient;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

@Value
public class AggregateState implements Message {
  String id;
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  String aggregateId;
  String eventId;
  long version;

  private AggregateState() {
    this.id = null;
    this.timestamp = null;
    this.type = null;
    this.payload = null;
    this.metadata = null;
    this.aggregateId = null;
    this.eventId = null;
    this.version = 0;
  }

  @Builder
  private AggregateState(Instant timestamp, Object payload, Metadata metadata, String eventId, long version) {
    this.timestamp = Optional.ofNullable(timestamp).orElse(Instant.now());
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.builder().build());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateId = IdUtils.getAggregateId(getPayload());
    this.id = IdUtils.createCompoundKey(getAggregateId(), getTimestamp());

    this.eventId = eventId;
    this.version = version;
  }

  public static class AggregateStateBuilder {
    Metadata.MetadataBuilder metadataBuilder = Metadata.builder();

    public AggregateStateBuilder metadata(String key, String value) {
      metadataBuilder = metadataBuilder.put(key, value);
      return this;
    }

    public AggregateStateBuilder metadata(Map<String, String> metadata) {
      if (metadata != null) {
        metadataBuilder = metadataBuilder.putAll(metadata);
      }
      return this;
    }

    public AggregateState build() {
      Metadata metadata = metadataBuilder.build();
      return new AggregateState(timestamp, payload, metadata, eventId, version);
    }
  }


  @Transient
  public int getSnapshotThreshold() {
    return Optional.ofNullable(getPayload())
        .map(Object::getClass)
        .map(aClass -> AnnotationScanner.findAnnotation(aClass, EnableSnapshotting.class))
        .map(EnableSnapshotting::threshold)
        .filter(threshold -> threshold > 0)
        .orElse(0);
  }

  @Transient
  public boolean deleteEvents() {
    return Optional.ofNullable(getPayload())
        .map(Object::getClass)
        .map(aClass -> AnnotationScanner.findAnnotation(aClass, EnableSnapshotting.class))
        .map(EnableSnapshotting::deleteEvents)
        .orElse(false);
  }
}
