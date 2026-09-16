package io.github.alikelleci.eventify.console.client;

import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateReplay;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.core.util.IdUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The history of an aggregate: its events, and its state at any of them, rebuilt the way the application rebuilds it
 * ({@link AggregateReplay}), from the snapshot whenever that gives the same answer.
 *
 * <p>The events of an aggregate are read from its key range, which can also hold another aggregate's events (see
 * {@link IdUtils#isKeyOf}): those are skipped everywhere.
 *
 * <p>A snapshot is the state after one event, and the store keeps only the latest one per aggregate. When the
 * aggregate deletes its events at a snapshot ({@code @EnableSnapshotting(deleteEvents = true)}), the events before
 * that event are gone: the snapshot is then the only way to know the state at it, and the state before it is unknown.
 */
class AggregateHistory {

  private final AggregateReplay replay;

  AggregateHistory(Map<Class<?>, EventSourcingHandler> eventSourcingHandlers) {
    this.replay = new AggregateReplay(eventSourcingHandlers);
  }

  /**
   * A page of the aggregate's events, newest first.
   *
   * @param cursor where the page starts: the {@link ConsoleService.EventsPage#nextCursor()} of the page before it, or
   *               {@code null} for the newest events
   */
  ConsoleService.EventsPage events(ReadOnlyKeyValueStore<String, Event> events, String aggregateId, String cursor, int limit) {
    String from = IdUtils.firstKey(aggregateId);
    String to = cursor != null ? IdUtils.firstKey(aggregateId) + cursor + "\0" : IdUtils.lastKey(aggregateId); // the cursor's event included

    // One more than the page, to know whether there is a next page and where it starts.
    List<Event> page = new ArrayList<>();
    try (KeyValueIterator<String, Event> iterator = events.reverseRange(from, to)) {
      while (iterator.hasNext() && page.size() <= limit) {
        KeyValue<String, Event> entry = iterator.next();
        if (IdUtils.isKeyOf(aggregateId, entry.key)) {
          page.add(entry.value);
        }
      }
    }

    String nextCursor = null;
    if (page.size() > limit) {
      Event first = page.remove(page.size() - 1);
      nextCursor = first.getId().substring(IdUtils.firstKey(aggregateId).length());
    }
    return new ConsoleService.EventsPage(page, nextCursor);
  }

  /** The aggregate's events with this correlation id, oldest first: the events one command produced. */
  List<Event> eventsByCorrelation(ReadOnlyKeyValueStore<String, Event> events, String aggregateId, String correlationId) {
    List<Event> correlated = new ArrayList<>();
    try (KeyValueIterator<String, Event> iterator = events.range(IdUtils.firstKey(aggregateId), IdUtils.lastKey(aggregateId))) {
      while (iterator.hasNext()) {
        KeyValue<String, Event> entry = iterator.next();
        if (IdUtils.isKeyOf(aggregateId, entry.key) && correlationId.equals(entry.value.getMetadata().get(Metadata.CORRELATION_ID))) {
          correlated.add(entry.value);
        }
      }
    }
    return correlated;
  }

  /**
   * The state after the event, or the current state when {@code eventId} is {@code null}; {@code null} when there is
   * none, or the event isn't there.
   */
  AggregateState stateAt(ReadOnlyKeyValueStore<String, Event> events, ReadOnlyKeyValueStore<String, AggregateState> snapshots,
                         String aggregateId, String eventId) {
    if (eventId != null && events.get(eventId) == null) {
      return null;
    }
    AggregateState snapshot = snapshots.get(aggregateId);
    // A snapshot after the event can't be the start: replay from the first event.
    if (snapshot != null && eventId != null && snapshot.getEventId().compareTo(eventId) > 0) {
      snapshot = null;
    }
    return replay.replay(events, aggregateId, snapshot, eventId).state();
  }

  /** The event with the state before and after it; {@code null} when the event isn't there. */
  ConsoleService.EventDetail eventDetail(ReadOnlyKeyValueStore<String, Event> events, ReadOnlyKeyValueStore<String, AggregateState> snapshots,
                                          String aggregateId, String eventId) {
    Event event = events.get(eventId);
    if (event == null) {
      return null;
    }

    AggregateState snapshot = snapshots.get(aggregateId);
    int order = snapshot != null ? snapshot.getEventId().compareTo(eventId) : 1;

    if (order < 0) {
      // The snapshot is before the event: start there. The state before the event is seen on the way.
      return replayThrough(events, aggregateId, snapshot, event);
    }

    // No snapshot before the event: from the first event.
    ConsoleService.EventDetail detail = replayThrough(events, aggregateId, null, event);
    if (order > 0) {
      return detail;
    }

    // The snapshot is the state after this very event. The replay only agrees with it when every earlier event is
    // still there; if they were deleted at this snapshot, it applied fewer events. Then the snapshot is the state,
    // and the state before it is unknown.
    long replayed = detail.state() != null ? detail.state().getVersion() : 0;
    if (replayed != snapshot.getVersion()) {
      return new ConsoleService.EventDetail(event, snapshot.withVersion(snapshot.getVersion()), null);
    }
    return detail;
  }

  /** Replays from {@code start} through the event, remembering the state right before it. */
  private ConsoleService.EventDetail replayThrough(ReadOnlyKeyValueStore<String, Event> events, String aggregateId,
                                                    AggregateState start, Event event) {
    AggregateState[] before = {start};
    AggregateReplay.Result result = replay.replay(events, aggregateId, start, event.getId(), (current, state, version) -> {
      if (current.getId().equals(event.getId())) {
        before[0] = state != null ? state.withVersion(version) : null;
      }
    });
    return new ConsoleService.EventDetail(event, result.state(), before[0]);
  }
}
