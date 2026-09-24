package io.github.alikelleci.eventify.core.aggregate;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.Revision;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.time.Instant;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AggregateState {
  /** Timestamp of the last applied event. */
  Instant timestamp;
  String type;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  Object payload;
  Metadata metadata;
  String aggregateId;
  /** Version of the last applied event. */
  long version;
  /** Aggregate revision used to create this state. */
  int revision;

  /** Empty state used when no snapshot is available. */
  public static AggregateState empty(String aggregateId) {
    return new AggregateState(Instant.EPOCH, null, null, Metadata.EMPTY, aggregateId, 0, 0);
  }

  /** State after an event; a null payload means removed. */
  static AggregateState after(Event event, Object payload, int revision) {
    String type = payload != null ? payload.getClass().getSimpleName() : null;
    return new AggregateState(event.getTimestamp(), type, payload, event.getMetadata(), event.getAggregateId(), event.getSequence(), revision);
  }
}
