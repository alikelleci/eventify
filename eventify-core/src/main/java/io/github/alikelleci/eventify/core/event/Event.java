package io.github.alikelleci.eventify.core.event;

import io.github.alikelleci.eventify.core.event.annotation.Revision;
import io.github.alikelleci.eventify.core.handler.internal.AnnotationScanner;
import io.github.alikelleci.eventify.core.message.Message;
import io.github.alikelleci.eventify.core.message.MessageIds;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.exception.PayloadMissingException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.github.alikelleci.eventify.core.message.Metadata.CORRELATION_ID;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Event implements Message {
  String id;
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  String aggregateId;
  int revision;

  @Builder
  private Event(Instant timestamp, Object payload, Metadata metadata) {
    this.timestamp = Optional.ofNullable(timestamp).orElse(Instant.now());
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.builder().build());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateId = AggregateIdResolver.getAggregateId(getPayload());
    this.id = MessageIds.createCompoundKey(getAggregateId());

    this.revision = Optional.ofNullable(AnnotationScanner.findAnnotation(getPayload().getClass(), Revision.class))
        .map(Revision::value)
        .orElse(1);

    getMetadata().putIfAbsent(CORRELATION_ID, UUID.randomUUID().toString());
  }

  /** This event under another key of the same aggregate. */
  public Event withId(String id) {
    if (!MessageIds.isKeyOf(aggregateId, id)) {
      throw new IllegalArgumentException("Key '" + id + "' is not a key of aggregate '" + aggregateId + "'.");
    }
    return new Event(id, timestamp, type, payload, metadata, aggregateId, revision);
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
