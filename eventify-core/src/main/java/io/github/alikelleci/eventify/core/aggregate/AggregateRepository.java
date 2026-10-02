package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventStore;

import java.util.List;

/** Read-only access to the history and reconstructed state of one aggregate type. */
public interface AggregateRepository {

  /** Replay start, resulting state and number of applied events. */
  record ReplayResult(AggregateState usedSnapshot, AggregateState currentState, long eventsReplayed) {
  }

  /** Returns the aggregate type. */
  String getAggregateType();

  /** Rebuilds the current state; its payload is null before creation or after removal. */
  ReplayResult replay(String aggregateId);

  /** Rebuilds state up to a sequence; {@code null} when pruned history prevents it. */
  AggregateState stateAt(String aggregateId, long sequence);

  /** Applies events in memory. */
  AggregateState applyEvents(AggregateState state, List<Event> events);

  /** Returns the event at a sequence, or {@code null}. */
  Event event(String aggregateId, long sequence);

  /** Returns stored events oldest first. Close the iterator. */
  EventStore.EventIterator events(String aggregateId);

  /** Returns stored events newest first. Close the iterator. */
  EventStore.EventIterator eventsNewestFirst(String aggregateId, long from, long to);
}
