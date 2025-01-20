package io.github.alikelleci.eventify.core.messaging.eventhandling;

import io.github.alikelleci.eventify.core.common.annotations.Revision;
import io.github.alikelleci.eventify.core.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.core.messaging.Message;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.util.AnnotationUtils;
import io.github.alikelleci.eventify.core.util.IdUtils;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Value
public class Event implements Message {
  String id;
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  String aggregateId;
  int revision;

  private Event() {
    this.id = null;
    this.timestamp = null;
    this.type = null;
    this.payload = null;
    this.metadata = null;
    this.aggregateId = null;
    this.revision = 1;
  }

  @Builder
  private Event(Instant timestamp, Object payload, Metadata metadata) {
    this.timestamp = Optional.ofNullable(timestamp).orElse(Instant.now());
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.builder().build());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateId = IdUtils.getAggregateId(getPayload());
    this.id = IdUtils.createCompoundKey(getAggregateId(), getTimestamp());

    this.revision = Optional.ofNullable(AnnotationUtils.findAnnotation(getPayload().getClass(), Revision.class))
        .map(Revision::value)
        .orElse(1);

    getMetadata().putIfAbsent(Metadata.CORRELATION_ID, UUID.randomUUID().toString());
  }

  public static class EventBuilder {
    Metadata.MetadataBuilder metadataBuilder = Metadata.builder();

    public EventBuilder metadata(String key, String value) {
      metadataBuilder.put(key, value);
      return this;
    }

    public EventBuilder metadata(Map<String, String> metadata) {
      if (metadata != null) {
        metadataBuilder = metadataBuilder.putAll(metadata);
      }
      return this;
    }

    public Event build() {
      Metadata metadata = metadataBuilder.build();
      return new Event(timestamp, payload, metadata);
    }
  }
}
