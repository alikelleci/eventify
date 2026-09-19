package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.aggregate.exception.EventReplayException;
import io.github.alikelleci.eventify.core.aggregate.internal.ApplyEventMethod;
import io.github.alikelleci.eventify.core.event.Event;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;

/**
 * Rebuilds the state of an aggregate: applies its stored events, in the order they are stored, to a starting state.
 *
 * <p>The events are applied in the order of their sequence: the order they were handled in. The version of the state
 * is the sequence of the last event applied: every event counts.
 *
 * <p>An event without an event sourcing handler leaves the state as it was, the way a handler that returns the state
 * it is given does: the state moves on to that event, so the version and the snapshot point to it too. Adding such a
 * handler later changes nothing.
 */
@Slf4j
public class AggregateReplayer {

  private final Map<Class<?>, ApplyEventMethod> eventSourcingHandlers;

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
     * @param state         the state before this event, without its version set; {@code null} when there is none
     * @param versionBefore the version of that state: one below this event's sequence
     */
    void beforeEvent(Event event, AggregateState state, long versionBefore);
  }

  public AggregateReplayer(Map<Class<?>, ApplyEventMethod> eventSourcingHandlers) {
    this.eventSourcingHandlers = eventSourcingHandlers;
  }

  /** @see #replay(Iterator, AggregateState, Listener) */
  public Result replay(Iterator<Event> events, AggregateState start) {
    return replay(events, start, null);
  }

  /**
   * Applies the events to the starting state.
   *
   * @param events   the aggregate's events after {@code start}, in the order they were handled: e.g. from
   *                 {@code ReadOnlyEventStore.events(aggregateId, start.getVersion() + 1, untilSequence)}
   * @param start    the state to start from, e.g. a snapshot; {@code null} to start before the first event
   * @param listener told about each event before it is applied; may be {@code null}
   */
  public Result replay(Iterator<Event> events, AggregateState start, Listener listener) {
    AggregateState state = start;
    long version = start != null ? start.getVersion() : 0;
    long replayed = 0;

    while (events.hasNext()) {
      Event event = events.next();
      if (event.getPayload() == null) {
        // Read without its class, which was renamed or removed. Not skipped: the state would silently miss the event.
        // Fails the replay instead, so every command of this aggregate fails with this reason until an upcaster fixes it.
        throw new EventReplayException("Stored event " + event.getId() + " (" + event.getType() + ") cannot be replayed: its class no longer exists. Add an upcaster that renames it to its current class.");
      }
      // A gap, or an event twice: the stored events are not the ones that were handled, and the state would be wrong
      // without a word. Fails the replay instead.
      if (event.getSequence() != version + 1) {
        throw new EventReplayException("The stored events of aggregate " + event.getAggregateId() + " are incomplete or out of order: expected #" + (version + 1) + ", found #" + event.getSequence() + " (" + event.getType() + ", event " + event.getId() + ").");
      }
      if (listener != null) {
        listener.beforeEvent(event, state, version);
      }
      state = apply(state, event);
      version = event.getSequence();
      replayed++;
    }

    return new Result(state != null ? state.withVersion(version) : null, replayed);
  }

  /**
   * The state after one event: what its event sourcing handler returns, or the state unchanged when it has none. The
   * version is not set.
   */
  public AggregateState apply(AggregateState state, Event event) {
    ApplyEventMethod handler = eventSourcingHandlers.get(event.getPayload().getClass());
    if (handler != null) {
      log.trace("Applying event: {} ({})", event.getType(), event.getAggregateId());
      return handler.apply(state, event);
    }
    log.trace("No Event Sourcing Handler found for event: {} ({}), state unchanged", event.getType(), event.getAggregateId());
    return state != null ? state.after(event) : null;
  }
}
