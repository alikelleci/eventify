package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.RawValue;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateReplay;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.exceptions.AggregateInvocationException;
import io.github.alikelleci.eventify.core.util.HandlerUtils;
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
 *
 * <p>A state is answered as JSON, written when it is taken during the replay. An event sourcing handler may change the
 * state it is given and return it: a state kept as an object would then change with every event after it.
 */
class AggregateHistory {

  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;
  private final AggregateReplay replay;
  /** Eventify's own mapper: the states are answered as the application writes them. */
  private final ObjectMapper objectMapper;

  AggregateHistory(Map<Class<?>, EventSourcingHandler> eventSourcingHandlers, ObjectMapper objectMapper) {
    this.eventSourcingHandlers = eventSourcingHandlers;
    this.replay = new AggregateReplay(eventSourcingHandlers);
    this.objectMapper = objectMapper;
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

  /**
   * The events the command produced, oldest first: the ones that name it as their cause. Not by correlation id: that is
   * shared with the other commands of the same flow, e.g. a saga. Only an event stored before events named their cause
   * is found by its correlation id, when one is given.
   */
  List<Event> eventsOfCommand(ReadOnlyKeyValueStore<String, Event> events, String aggregateId, String commandId, String correlationId) {
    List<Event> produced = new ArrayList<>();
    try (KeyValueIterator<String, Event> iterator = events.range(IdUtils.firstKey(aggregateId), IdUtils.lastKey(aggregateId))) {
      while (iterator.hasNext()) {
        KeyValue<String, Event> entry = iterator.next();
        if (IdUtils.isKeyOf(aggregateId, entry.key) && isCausedBy(entry.value.getMetadata(), commandId, correlationId)) {
          produced.add(entry.value);
        }
      }
    }
    return produced;
  }

  private static boolean isCausedBy(Metadata metadata, String commandId, String correlationId) {
    String causationId = metadata.getCausationId();
    if (causationId != null) {
      return causationId.equals(commandId);
    }
    return correlationId != null && correlationId.equals(metadata.getCorrelationId());
  }

  /**
   * The state after the event, or the current state when {@code eventId} is {@code null}; {@code null} when there is
   * none, the event isn't there, or the state at it is unknown because earlier events were deleted.
   */
  RawValue stateAt(ReadOnlyKeyValueStore<String, Event> events, ReadOnlyKeyValueStore<String, AggregateState> snapshots,
                   String aggregateId, String eventId) {
    if (eventId != null && events.get(eventId) == null) {
      return null;
    }
    AggregateState snapshot = snapshots.get(aggregateId);
    if (snapshot == null || eventId == null || snapshot.getEventId().compareTo(eventId) <= 0) {
      return json(replay.replay(events, aggregateId, snapshot, eventId).state());
    }
    // A snapshot after the event can't be the start: replay from the first event.
    FromFirst fromFirst = replayFromFirst(events, aggregateId, snapshot, eventId);
    return fromFirst.complete() ? fromFirst.after() : null;
  }

  /** The event with the state before and after it; {@code null} when the event isn't there. */
  ConsoleService.EventDetail eventDetail(ReadOnlyKeyValueStore<String, Event> events, ReadOnlyKeyValueStore<String, AggregateState> snapshots,
                                          String aggregateId, String eventId) {
    Event event = events.get(eventId);
    if (event == null) {
      return null;
    }

    AggregateState snapshot = snapshots.get(aggregateId);
    if (snapshot != null && snapshot.getEventId().compareTo(eventId) < 0) {
      // The snapshot is before the event: start there. The state before the event is seen on the way.
      RawValue[] before = {null};
      AggregateReplay.Result result = replay.replay(events, aggregateId, snapshot, eventId, (current, state, version) -> {
        if (current.getId().equals(eventId)) {
          before[0] = versioned(state, version);
        }
      });
      return known(event, json(result.state()), before[0]);
    }

    // No snapshot before the event: from the first event.
    FromFirst fromFirst = replayFromFirst(events, aggregateId, snapshot, eventId);
    if (fromFirst.complete()) {
      return known(event, fromFirst.after(), fromFirst.before());
    }
    if (snapshot.getEventId().equals(eventId)) {
      // The snapshot is the state after this very event; what came before it is gone.
      return new ConsoleService.EventDetail(event, json(snapshot), null, true, false);
    }
    return new ConsoleService.EventDetail(event, null, null, false, false);
  }

  /**
   * The state before and after the event, replayed from the first event. With a snapshot, the replay goes on to the
   * snapshot's event: only when it reaches the snapshot's version are all events before the snapshot still there. When
   * some were deleted ({@code @EnableSnapshotting(deleteEvents = true)}), they are the first ones: the replay started
   * too late, and its states are wrong.
   *
   * @param snapshot the snapshot, at or after the event; {@code null} when there is none
   */
  private FromFirst replayFromFirst(ReadOnlyKeyValueStore<String, Event> events, String aggregateId,
                                    AggregateState snapshot, String eventId) {
    RawValue[] before = {null};
    RawValue[] after = {null};
    boolean[] reached = {false};
    boolean[] passed = {false};
    String until = snapshot != null ? snapshot.getEventId() : eventId;

    AggregateReplay.Result result;
    try {
      result = replay.replay(events, aggregateId, null, until, (current, state, version) -> {
        if (reached[0] && !passed[0]) {
          after[0] = versioned(state, version); // the state before the next event is the state after the event
          passed[0] = true;
        }
        if (current.getId().equals(eventId)) {
          before[0] = versioned(state, version);
          reached[0] = true;
        }
      });
    } catch (AggregateInvocationException e) {
      // With events deleted, the first event left is applied without the state before it, which a handler may refuse.
      if (snapshot != null && handledEventsUntil(events, aggregateId, snapshot.getEventId()) < snapshot.getVersion()) {
        return new FromFirst(null, null, false);
      }
      throw e; // all events are there: the handler itself fails
    }
    if (!passed[0]) {
      after[0] = json(result.state());
    }

    boolean complete = snapshot == null
        || (result.state() != null && result.state().getVersion() == snapshot.getVersion());
    return new FromFirst(before[0], after[0], complete);
  }

  /** How many of the aggregate's stored events, up to and including this one, have an event sourcing handler. */
  private long handledEventsUntil(ReadOnlyKeyValueStore<String, Event> events, String aggregateId, String untilEventId) {
    long count = 0;
    try (KeyValueIterator<String, Event> iterator = events.range(IdUtils.firstKey(aggregateId), untilEventId)) {
      while (iterator.hasNext()) {
        KeyValue<String, Event> entry = iterator.next();
        if (IdUtils.isKeyOf(aggregateId, entry.key) && HandlerUtils.findHandler(eventSourcingHandlers, entry.value.getPayload().getClass()) != null) {
          count++;
        }
      }
    }
    return count;
  }

  private record FromFirst(RawValue before, RawValue after, boolean complete) {
  }

  private static ConsoleService.EventDetail known(Event event, RawValue state, RawValue previousState) {
    return new ConsoleService.EventDetail(event, state, previousState, true, true);
  }

  /** The state at this version, as JSON written now: before the next handler can change it. */
  private RawValue versioned(AggregateState state, long version) {
    return state != null ? json(state.withVersion(version)) : null;
  }

  private RawValue json(AggregateState state) {
    if (state == null) {
      return null;
    }
    try {
      return new RawValue(objectMapper.writeValueAsString(state));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot write the state of aggregate '" + state.getAggregateId() + "' as JSON", e);
    }
  }
}
