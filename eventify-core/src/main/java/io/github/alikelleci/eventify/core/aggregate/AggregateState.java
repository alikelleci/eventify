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
  /** When the last event applied to this state was recorded. */
  Instant timestamp;
  String type;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  Object payload;
  Metadata metadata;
  String aggregateId;
  /** The sequence of the last applied event. */
  long version;
  /** The {@link Revision} of the aggregate class that made this state. */
  int revision;

  /** A replay starts here when no usable snapshot exists. */
  public static AggregateState empty(String aggregateId) {
    return new AggregateState(Instant.EPOCH, null, null, Metadata.EMPTY, aggregateId, 0, 0);
  }

  /**
   * The state after an event, with that event's envelope; a null payload means removed.
   * Package-private: only the repository knows the aggregate's revision.
   */
  static AggregateState after(Event event, Object payload, int revision) {
    String type = payload != null ? payload.getClass().getSimpleName() : null;
    return new AggregateState(event.getTimestamp(), type, payload, event.getMetadata(), event.getAggregateId(), event.getSequence(), revision);
  }
}
