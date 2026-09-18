package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.aggregate.internal.ApplyEventMethod;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.MessageIds;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;

/**
 * Rebuilds the state of an aggregate: applies its stored events, in the order they are stored, to a starting state.
 *
 * <p>The events are applied in the order they were handled, the order they are stored in (see
 * {@link MessageIds#nextEventKey}). The version of the state is its position in the aggregate's events: every event
 * counts.
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
     * @param state   the state before this event, without its version set; {@code null} when there is none
     * @param version the version of that state
     */
    void beforeEvent(Event event, AggregateState state, long version);
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
   *                 {@code ReadOnlyEventStore.events(aggregateId, start.getEventId(), untilEventId)}
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
        throw new IllegalStateException("Stored event " + event.getId() + " (" + event.getType() + ") cannot be replayed: its class no longer exists. Add an upcaster that renames it to its current class.");
      }
      if (listener != null) {
        listener.beforeEvent(event, state, version);
      }
      ApplyEventMethod handler = eventSourcingHandlers.get(event.getPayload().getClass());
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

    return new Result(state != null ? state.withVersion(version) : null, replayed);
  }
}
