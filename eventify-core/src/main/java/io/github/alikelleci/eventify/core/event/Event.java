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
import java.util.Optional;
import java.util.UUID;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CORRELATION_ID;

/** Something that happened to an aggregate; complete from creation, including its sequence. */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Event implements Message {
  String id;
  /** When Eventify recorded it; business time belongs in the payload. */
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  /** The aggregate this event belongs to, as its {@code @AggregateRoot} names it, e.g. "order". */
  String aggregateType;
  String aggregateId;
  int revision;
  /** Position in its aggregate, starting at 1. */
  long sequence;

  @Builder
  private Event(String aggregateType, Object payload, Metadata metadata, long sequence) {
    this.timestamp = Instant.now();
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.EMPTY)
        .withDefault(CORRELATION_ID, UUID.randomUUID().toString());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateType = Optional.ofNullable(aggregateType).filter(name -> !name.isBlank())
        .orElseThrow(() -> new IllegalArgumentException("Event " + this.type + " has no aggregate: an event is made for the aggregate it belongs to."));
    this.aggregateId = AggregateIdResolver.getAggregateId(getPayload());
    this.id = UUID.randomUUID().toString();

    this.revision = Revisions.of(getPayload().getClass());
    if (sequence < 1) {
      throw new IllegalArgumentException("Event " + this.type + " of aggregate " + this.aggregateId + " has no sequence: an event is made with the place it has in its aggregate.");
    }
    this.sequence = sequence;
  }

}
