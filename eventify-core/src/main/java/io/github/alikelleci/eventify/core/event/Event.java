package io.github.alikelleci.eventify.core.event;

import io.github.alikelleci.eventify.core.message.Message;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.exception.PayloadMissingException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import io.github.alikelleci.eventify.core.message.internal.Revisions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CORRELATION_ID;

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
  /**
   * The event's position in its aggregate: 1 for its first event, then one more for each next one. Set when the event
   * is stored; 0 before that, and in events stored before sequences existed.
   */
  long sequence;

  @Builder
  private Event(Instant timestamp, Object payload, Metadata metadata) {
    this.timestamp = Optional.ofNullable(timestamp).orElse(Instant.now());
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.builder().build());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateId = AggregateIdResolver.getAggregateId(getPayload());
    this.id = UUID.randomUUID().toString();

    this.revision = Revisions.of(getPayload().getClass());
    this.sequence = 0; // given when stored

    getMetadata().putIfAbsent(CORRELATION_ID, UUID.randomUUID().toString());
  }

  /** This event at the given position in its aggregate. */
  public Event withSequence(long sequence) {
    return new Event(id, timestamp, type, payload, metadata, aggregateId, revision, sequence);
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
