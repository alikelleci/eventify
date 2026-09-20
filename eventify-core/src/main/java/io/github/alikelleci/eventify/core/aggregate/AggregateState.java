package io.github.alikelleci.eventify.core.aggregate;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.Revision;
import io.github.alikelleci.eventify.core.message.internal.Revisions;
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
  /** The sequence of the last event applied to this state: how many events the aggregate had then. */
  long version;
  /**
   * The {@link Revision} of the aggregate class this state was made with: of its fields and its event sourcing
   * handlers. A snapshot made with another revision is not used. 0 in a snapshot made before revisions were stored: it
   * counts as 1, the revision of a class without {@code @Revision}.
   */
  int revision;

  /** A replay starts here when no usable snapshot exists. */
  public static AggregateState empty(String aggregateId) {
    return new AggregateState(Instant.EPOCH, null, null, Metadata.EMPTY, aggregateId, 0, 0);
  }

  /**
   * The state after an event. Its envelope always comes from that event. A null payload represents a removed
   * aggregate; otherwise type and revision describe the resulting aggregate payload.
   */
  public static AggregateState after(Event event, Object payload) {
    if (payload == null) {
      return new AggregateState(event.getTimestamp(), null, null, event.getMetadata(), event.getAggregateId(), event.getSequence(), 0);
    }
    return new AggregateState(event.getTimestamp(), payload.getClass().getSimpleName(), payload, event.getMetadata(),
        event.getAggregateId(), event.getSequence(), Revisions.of(payload.getClass()));
  }
}
