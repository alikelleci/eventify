package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.common.annotations.EnableSnapshotting;
import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.util.IdUtils;
import lombok.Builder;
import lombok.Value;
import org.springframework.core.annotation.AnnotationUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.Optional;

@Value
public class Aggregate implements Message {
  String id;
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  String aggregateId;
  String eventId;
  long version;

  private Aggregate() {
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
  private Aggregate(Instant timestamp, Object payload, Metadata metadata, String eventId, long version) {
    this.timestamp = Optional.ofNullable(timestamp).orElse(Instant.now());
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.builder().build());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateId = IdUtils.getAggregateId(getPayload());
    this.id = IdUtils.createCompoundKey(getAggregateId(), getTimestamp());

    this.eventId = eventId;
    this.version = version;
  }

  public static class AggregateBuilder {
    Metadata.MetadataBuilder metadataBuilder = Metadata.builder();

    public AggregateBuilder metadata(String key, String value) {
      this.metadataBuilder.add(key, value);
      return this;
    }

    public AggregateBuilder metadata(Metadata metadata) {
      if (metadata != null) {
        metadataBuilder.addAll(metadata);
      }
      return this;
    }

    public Aggregate build() {
      Metadata metadata = metadataBuilder.build();
      return new Aggregate(timestamp, payload, metadata, eventId, version);
    }
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
