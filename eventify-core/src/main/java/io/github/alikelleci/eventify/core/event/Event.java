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

/**
 * Something that happened to an aggregate. An event is made complete: it knows its place in its aggregate from the
 * moment it exists, so there is no event that still has to be finished before it can be stored.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Event implements Message {
  String id;
  /** When Eventify recorded this event. Not when what it tells about happened: that belongs in the payload. */
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  String aggregateId;
  int revision;
  /** The event's position in its aggregate: 1 for its first event, then one more for each next one. */
  long sequence;

  @Builder
  private Event(Instant timestamp, Object payload, Metadata metadata, long sequence) {
    this.timestamp = Optional.ofNullable(timestamp).orElse(Instant.now());
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    // A copy with the flow this event belongs to: the metadata that was given stays as it is.
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.EMPTY)
        .withDefault(CORRELATION_ID, UUID.randomUUID().toString());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateId = AggregateIdResolver.getAggregateId(getPayload());
    this.id = UUID.randomUUID().toString();

    this.revision = Revisions.of(getPayload().getClass());
    if (sequence < 1) {
      throw new IllegalArgumentException("Event " + this.type + " of aggregate " + this.aggregateId + " has no sequence: an event is made with the place it has in its aggregate.");
    }
    this.sequence = sequence;
  }

  public static class EventBuilder {
    Metadata metadata = Metadata.EMPTY;

    public EventBuilder metadata(String key, String value) {
      metadata = metadata.with(key, value);
      return this;
    }

    public EventBuilder metadata(Map<String, String> metadata) {
      this.metadata = this.metadata.with(metadata);
      return this;
    }

    public Event build() {
      return new Event(timestamp, payload, metadata, sequence);
    }
  }
}
