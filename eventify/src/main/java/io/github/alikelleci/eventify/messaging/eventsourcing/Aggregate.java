package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.util.IdUtils;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Map;
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

  private Aggregate() {
    this.id = null;
    this.timestamp = null;
    this.type = null;
    this.payload = null;
    this.metadata = null;
    this.aggregateId = null;
    this.eventId = null;
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
  }

  public static class AggregateBuilder {
    Metadata.MetadataBuilder metadataBuilder = Metadata.builder();

    public AggregateBuilder metadata(String key, String value) {
      metadataBuilder = metadataBuilder.put(key, value);
      return this;
    }

    public AggregateBuilder metadata(Map<String, String> metadata) {
      if (metadata != null) {
        metadataBuilder = metadataBuilder.putAll(metadata);
      }
      return this;
    }

    public Aggregate build() {
      Metadata metadata = metadataBuilder.build();
      return new Aggregate(timestamp, payload, metadata, eventId, version);
    }
  }
}
