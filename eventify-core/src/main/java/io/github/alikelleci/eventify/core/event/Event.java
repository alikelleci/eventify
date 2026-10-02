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

/** An immutable fact recorded for an aggregate. */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Event implements Message {
  String id;
  /** Time at which Eventify recorded the event. */
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  /** Aggregate type, for example {@code order}. */
  String aggregateType;
  String aggregateId;
  int revision;
  /** Aggregate sequence, starting at 1. */
  long sequence;

  @Builder
  private Event(String aggregateType, Object payload, Metadata metadata, long sequence) {
    this.timestamp = Instant.now();
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.EMPTY)
        .withDefault(CORRELATION_ID, UUID.randomUUID().toString());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateType = Optional.ofNullable(aggregateType).filter(name -> !name.isBlank())
        .orElseThrow(() -> new IllegalArgumentException("Event " + this.type + " needs an aggregate type."));
    this.aggregateId = AggregateIdResolver.getAggregateId(getPayload());
    this.id = UUID.randomUUID().toString();

    this.revision = Revisions.of(getPayload().getClass());
    if (sequence < 1) {
      throw new IllegalArgumentException("Event " + this.type + " needs a sequence of 1 or higher, not " + sequence + ".");
    }
    this.sequence = sequence;
  }

}
