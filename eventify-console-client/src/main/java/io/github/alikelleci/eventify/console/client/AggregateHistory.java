package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.RawValue;
import io.github.alikelleci.eventify.core.aggregate.AggregateReplayer;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.EventStore;
import io.github.alikelleci.eventify.core.store.SnapshotStore;
import io.github.alikelleci.eventify.console.protocol.Requests;

import java.util.ArrayList;
import java.util.List;

/**
 * The history of an aggregate: its events, and its state at any of them, rebuilt the way the application rebuilds it
 * ({@link AggregateReplayer}), from the snapshot whenever that gives the same answer. An event is known by its
 * sequence: 1 for the aggregate's first event, then one more for each next one.
 *
 * <p>A snapshot is the state after one event, and the store keeps only the latest one per aggregate. When the
 * aggregate deletes its events at a snapshot ({@code @EnableSnapshotting(deleteEvents = true)}), the events before
 * that event are gone: the snapshot is then the only way to know the state at it, and the state before it is unknown.
 *
 * <p>A state is answered as JSON, written when it is taken during the replay. An event sourcing handler may change the
 * state it is given and return it: a state kept as an object would then change with every event after it.
 */
class AggregateHistory {

  private final AggregateReplayer replay;
  /** Eventify's own mapper: the states are answered as the application writes them. */
  private final ObjectMapper objectMapper;

  AggregateHistory(AggregateReplayer replay, ObjectMapper objectMapper) {
    this.replay = replay;
    this.objectMapper = objectMapper;
  }

  /**
   * A page of the aggregate's events, newest first.
   *
   * @param cursor the sequence the page starts at, included: the {@link ConsoleViews.EventsPage#nextCursor()} of the
   *               page before it, or {@code null} for the newest events
   */
  ConsoleViews.EventsPage events(EventStore events, String aggregateId, Long cursor, int limit) {
    // One more than the page, to know whether there is a next page and where it starts.
    List<Event> page = new ArrayList<>();
    try (EventStore.Events newestFirst = events.eventsNewestFirst(aggregateId, cursor != null ? cursor : Long.MAX_VALUE, 1)) {
      while (newestFirst.hasNext() && page.size() <= limit) {
        page.add(newestFirst.next());
      }
    }

    Long nextCursor = null;
    if (page.size() > limit) {
      nextCursor = page.remove(page.size() - 1).getSequence();
    }
    return new ConsoleViews.EventsPage(page, nextCursor);
  }

  /**
   * The events the command produced, oldest first: the ones whose {@code $causationId} is the command's id. Not by
   * correlation id: that is shared with the other commands of the same flow, e.g. a saga.
   */
  List<Event> eventsOfCommand(EventStore events, Requests.EventsOfCommand request) {
    List<Event> produced = new ArrayList<>();
    try (EventStore.Events all = events.events(request.aggregateId())) {
      while (all.hasNext()) {
        Event event = all.next();
        if (request.commandId().equals(event.getMetadata().getCausationId())) {
          produced.add(event);
        }
      }
    }
    return produced;
  }

  /**
   * The state after the event with this sequence, or the current state when it is {@code null}; {@code null} when there
   * is none, the event isn't there, or the state at it is unknown because earlier events were deleted.
   */
  RawValue stateAt(EventStore events, SnapshotStore snapshots, String aggregateId, Long sequence) {
    if (sequence != null && events.get(aggregateId, sequence) == null) {
      return null;
    }
    AggregateState snapshot = snapshots.getUsable(aggregateId);
    long until = sequence != null ? sequence : Long.MAX_VALUE;
    if (snapshot != null && snapshot.getVersion() <= until) {
      return json(replay(events, aggregateId, snapshot, until, null).state());
    }
    // No snapshot to start from, or one after the event: only a replay from the first event knows the state.
    if (!allEventsStored(events, aggregateId)) {
      return null;
    }
    return json(replay(events, aggregateId, null, until, null).state());
  }

  /** The event with the state before and after it; {@code null} when the event isn't there. */
  ConsoleViews.EventDetail eventDetail(EventStore events, SnapshotStore snapshots,
                                       String aggregateId, long sequence) {
    Event event = events.get(aggregateId, sequence);
    if (event == null) {
      return null;
    }

    AggregateState snapshot = snapshots.getUsable(aggregateId);
    if (snapshot != null && snapshot.getVersion() < sequence) {
      // The snapshot is before the event: start there. The state before the event is seen on the way.
      return withStates(event, events, aggregateId, snapshot, sequence);
    }
    if (allEventsStored(events, aggregateId)) {
      return withStates(event, events, aggregateId, null, sequence);
    }
    if (snapshot != null && snapshot.getVersion() == sequence) {
      // The snapshot is the state after this very event; what came before it is gone.
      return ConsoleViews.EventDetail.withUnknownPreviousState(event, json(snapshot));
    }
    return ConsoleViews.EventDetail.withUnknownStates(event);
  }

  /** The event with the states before and after it, replayed from {@code start} up to the event. */
  private ConsoleViews.EventDetail withStates(Event event, EventStore events, String aggregateId,
                                              AggregateState start, long sequence) {
    RawValue[] before = {null};
    AggregateReplayer.Result result = replay(events, aggregateId, start, sequence, (current, state) -> {
      if (current.getSequence() == sequence) {
        // Written now, before the next event sourcing handler can change the state it is given.
        before[0] = json(state);
      }
    });
    return ConsoleViews.EventDetail.known(event, json(result.state()), before[0]);
  }

  /**
   * Whether the aggregate's first event is still stored: then every state can be replayed from the start. Not when it
   * deletes its events at a snapshot ({@code @EnableSnapshotting(deleteEvents = true)}): then its first stored event
   * comes after sequence 1.
   */
  private static boolean allEventsStored(EventStore events, String aggregateId) {
    try (EventStore.Events all = events.events(aggregateId)) {
      return !all.hasNext() || all.next().getSequence() == 1;
    }
  }

  /** The aggregate's events after {@code start}, up to and including {@code untilSequence}, applied to {@code start}. */
  private AggregateReplayer.Result replay(EventStore events, String aggregateId, AggregateState start,
                                          long untilSequence, AggregateReplayer.Listener listener) {
    try (EventStore.Events toApply = events.events(aggregateId, start != null ? start.getVersion() + 1 : 1, untilSequence)) {
      return replay.replay(toApply, start, listener);
    }
  }

  private RawValue json(AggregateState state) {
    if (state == null) {
      return null;
    }
    try {
      return new RawValue(objectMapper.writeValueAsString(state));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot write the state of " + state.getType() + " " + state.getAggregateId() + " as JSON", e);
    }
  }
}
