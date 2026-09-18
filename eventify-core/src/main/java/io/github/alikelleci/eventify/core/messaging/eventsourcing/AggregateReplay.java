package io.github.alikelleci.eventify.core.messaging.eventsourcing;

import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.util.IdUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Map;

/**
 * Rebuilds the state of an aggregate: applies its stored events, in the order they are stored, to a starting state.
 *
 * <p>Events are stored under {@code aggregateId@<ULID>}, so the events of one aggregate are in one key range, in the
 * order they were handled (see {@link IdUtils#nextEventKey}). That range can also hold another aggregate's events (see {@link IdUtils#isKeyOf}): those are
 * skipped. The version of the state is its position in the aggregate's events: every event counts.
 *
 * <p>An event without an event sourcing handler leaves the state as it was, the way a handler that returns the state
 * it is given does: the state moves on to that event, so the version and the snapshot point to it too. Adding such a
 * handler later changes nothing.
 */
@Slf4j
public class AggregateReplay {

  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;

  /**
   * The outcome of a replay.
   *
   * @param state   the state after the last event, with its version; {@code null} when there is no state, e.g. no
   *                events, or a handler that removed the aggregate
   * @param replayed how many events this replay went through, also the ones without a handler: the version minus the
   *                 starting state's version. Without a starting state it equals the version; with a snapshot, it is
   *                 what the snapshot saved to replay
   */
  public record Result(AggregateState state, long replayed) {
  }

  /** Told about every event in the replay, also the ones without a handler, before it is applied. */
  @FunctionalInterface
  public interface Listener {
    /**
     * @param event   the event about to be applied
     * @param state   the state before this event, without its version set; {@code null} when there is none
     * @param version the version of that state
     */
    void beforeEvent(Event event, AggregateState state, long version);
  }

  public AggregateReplay(Map<Class<?>, EventSourcingHandler> eventSourcingHandlers) {
    this.eventSourcingHandlers = eventSourcingHandlers;
  }

  /** @see #replay(ReadOnlyKeyValueStore, String, AggregateState, String, Listener) */
  public Result replay(ReadOnlyKeyValueStore<String, Event> eventStore, String aggregateId, AggregateState start, String untilEventId) {
    return replay(eventStore, aggregateId, start, untilEventId, null);
  }

  /**
   * Applies the aggregate's events that come after {@code start}, up to and including {@code untilEventId}.
   *
   * @param start        the state to start from, e.g. a snapshot; {@code null} to start before the first event
   * @param untilEventId the last event to apply; {@code null} for all events
   * @param listener     told about each event before it is applied; may be {@code null}
   * @throws IllegalArgumentException when {@code start} or {@code untilEventId} is not of this aggregate, or
   *                                  {@code untilEventId} comes before {@code start}: the replay would apply the wrong
   *                                  events
   */
  public Result replay(ReadOnlyKeyValueStore<String, Event> eventStore, String aggregateId, AggregateState start,
                       String untilEventId, Listener listener) {
    if (untilEventId != null && !IdUtils.isKeyOf(aggregateId, untilEventId)) {
      throw new IllegalArgumentException("Cannot load aggregate '" + aggregateId + "' up to event '" + untilEventId + "': that event belongs to another aggregate.");
    }
    if (start != null && !IdUtils.isKeyOf(aggregateId, start.getEventId())) {
      throw new IllegalArgumentException("Cannot load aggregate '" + aggregateId + "': its snapshot points to event '" + start.getEventId() + "', which belongs to another aggregate. The snapshot is damaged.");
    }

    AggregateState state = start;
    long version = start != null ? start.getVersion() : 0;
    long replayed = 0;

    if (start != null && untilEventId != null) {
      int order = start.getEventId().compareTo(untilEventId);
      if (order > 0) {
        throw new IllegalArgumentException("Cannot load aggregate '" + aggregateId + "' up to event '" + untilEventId + "': its snapshot is already past that event.");
      }
      if (order == 0) {
        // Nothing to apply. Not a range: its start would come after its end.
        return new Result(start.withVersion(version), 0);
      }
    }

    String from = start != null ? start.getEventId() + "\0" : IdUtils.firstKey(aggregateId); // after the starting state's event
    String to = untilEventId != null ? untilEventId : IdUtils.lastKey(aggregateId);

    try (KeyValueIterator<String, Event> iterator = eventStore.range(from, to)) {
      while (iterator.hasNext()) {
        KeyValue<String, Event> entry = iterator.next();
        if (!IdUtils.isKeyOf(aggregateId, entry.key)) {
          continue;
        }
        Event event = entry.value;
        if (event.getPayload() == null) {
          // Read without its class, which was renamed or removed. Not skipped: the state would silently miss the event.
          // Fails the replay instead, so every command of this aggregate fails with this reason until an upcaster fixes it.
          throw new IllegalStateException("Stored event " + entry.key + " (" + event.getType() + ") cannot be replayed: its class no longer exists. Add an upcaster that renames it to its current class.");
        }
        if (listener != null) {
          listener.beforeEvent(event, state, version);
        }
        EventSourcingHandler handler = eventSourcingHandlers.get(event.getPayload().getClass());
        if (handler != null) {
          log.trace("Applying event: {} ({})", event.getType(), event.getAggregateId());
          state = handler.apply(state, event);
        } else {
          log.trace("No Event Sourcing Handler found for event: {} ({}), state unchanged", event.getType(), event.getAggregateId());
          state = state != null ? state.after(event) : null;
        }
        version++;
        replayed++;
      }
    }

    return new Result(state != null ? state.withVersion(version) : null, replayed);
  }
}
