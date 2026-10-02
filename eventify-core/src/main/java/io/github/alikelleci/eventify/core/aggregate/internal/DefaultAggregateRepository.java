package io.github.alikelleci.eventify.core.aggregate.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateRepository;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.SnapshotStore;
import io.github.alikelleci.eventify.core.aggregate.exception.EventReplayException;
import io.github.alikelleci.eventify.core.aggregate.exception.SnapshotOutdatedException;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventStore;
import io.github.alikelleci.eventify.core.message.internal.Revisions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Replays one aggregate type from the event and snapshot stores. */
@Slf4j
public final class DefaultAggregateRepository implements AggregateRepository {

  private final EventStore eventStore;
  private final SnapshotStore snapshotStore;
  private final Map<Class<?>, EventSourcingHandlerMethod> eventSourcingHandlers;
  private final Class<?> aggregateClass;
  private final String aggregateType;
  private final int revision;
  private final SnapshotPolicy snapshotPolicy;

  public DefaultAggregateRepository(EventStore eventStore, SnapshotStore snapshotStore, Map<Class<?>, EventSourcingHandlerMethod> eventSourcingHandlers,
                                    Class<?> aggregateClass) {
    this.eventStore = eventStore;
    this.snapshotStore = snapshotStore;
    this.eventSourcingHandlers = eventSourcingHandlers;
    this.aggregateClass = aggregateClass;
    this.aggregateType = AggregateTypes.of(aggregateClass);
    this.revision = Revisions.of(aggregateClass);
    this.snapshotPolicy = SnapshotPolicy.of(aggregateClass);
  }

  /** One repository per aggregate type, by type name. */
  public static Map<String, DefaultAggregateRepository> forTypes(EventStore eventStore, SnapshotStore snapshotStore,
                                                                 Map<Class<?>, EventSourcingHandlerMethod> eventSourcingHandlers,
                                                                 Collection<Class<?>> aggregateClasses) {
    return aggregateClasses.stream()
        .map(aggregateClass -> new DefaultAggregateRepository(eventStore, snapshotStore, eventSourcingHandlers, aggregateClass))
        .collect(Collectors.toUnmodifiableMap(DefaultAggregateRepository::getAggregateType, repository -> repository));
  }

  @Override
  public String getAggregateType() {
    return aggregateType;
  }

  @Override
  public ReplayResult replay(String aggregateId) {
    long started = System.nanoTime();
    AggregateState snapshot = snapshotToStartFrom(aggregateId);
    AggregateState start = snapshot != null ? snapshot : AggregateState.empty(aggregateId);
    try (EventStore.EventIterator events = eventStore.events(aggregateType, aggregateId, start.getVersion() + 1, Long.MAX_VALUE)) {
      AggregateState state = applyEvents(aggregateId, start, events);
      long eventsReplayed = state.getVersion() - start.getVersion();
      // No payloads in the log: they may hold personal data.
      log.debug("Replayed {} events for {} ({}) to version {} in {} ms", eventsReplayed,
          aggregateType, aggregateId, state.getVersion(), (System.nanoTime() - started) / 1_000_000);
      return new ReplayResult(snapshot, state, eventsReplayed);
    }
  }

  @Override
  public AggregateState stateAt(String aggregateId, long sequence) {
    if (sequence < 0) {
      throw new IllegalArgumentException("Sequence can't be negative: " + sequence);
    }
    AggregateState storedSnapshot = snapshotStore.get(aggregateType, aggregateId);
    AggregateState start;
    if (storedSnapshot != null && storedSnapshot.getVersion() <= sequence && whySnapshotIsUnusable(aggregateId, storedSnapshot) == null) {
      start = storedSnapshot;
    } else if (hasCompleteHistory(aggregateId, storedSnapshot)) {
      start = AggregateState.empty(aggregateId);
    } else {
      return null;
    }
    if (start.getVersion() >= sequence) {
      return start;
    }
    try (EventStore.EventIterator events = eventStore.events(aggregateType, aggregateId, start.getVersion() + 1, sequence)) {
      return applyEvents(aggregateId, start, events);
    }
  }

  @Override
  public AggregateState applyEvents(AggregateState state, List<Event> events) {
    return applyEvents(state.getAggregateId(), state, events.iterator());
  }

  @Override
  public Event event(String aggregateId, long sequence) {
    return eventStore.get(aggregateType, aggregateId, sequence);
  }

  @Override
  public EventStore.EventIterator events(String aggregateId) {
    return eventStore.events(aggregateType, aggregateId);
  }

  @Override
  public EventStore.EventIterator eventsNewestFirst(String aggregateId, long from, long to) {
    return eventStore.eventsNewestFirst(aggregateType, aggregateId, from, to);
  }

  /** Returns whether a snapshot is due. */
  public boolean isSnapshotDue(long snapshotVersion, AggregateState state) {
    return snapshotPolicy.isSnapshotDue(snapshotVersion, state.getVersion());
  }

  /** Returns whether snapshots prune earlier events. */
  public boolean deletesEventsAtSnapshot() {
    return snapshotPolicy.deleteEvents();
  }

  /** Distinguishes complete history from history pruned at a snapshot. */
  private boolean hasCompleteHistory(String aggregateId, AggregateState storedSnapshot) {
    try (EventStore.EventIterator events = eventStore.events(aggregateType, aggregateId)) {
      if (events.hasNext()) {
        return events.next().getSequence() == 1;
      }
      // No events but a snapshot past version 0: the history was pruned.
      return storedSnapshot == null || storedSnapshot.getVersion() == 0;
    }
  }

  private AggregateState snapshotToStartFrom(String aggregateId) {
    AggregateState stored = snapshotStore.get(aggregateType, aggregateId);
    String whyUnusable = whySnapshotIsUnusable(aggregateId, stored);
    if (whyUnusable == null) {
      return stored;
    }
    if (!hasCompleteHistory(aggregateId, stored)) {
      throw new SnapshotOutdatedException("Snapshot of " + aggregateType + " " + aggregateId + " can't be used (" + whyUnusable
          + "), and the events before it were deleted, so the aggregate can't be rebuilt.");
    }
    log.info("Snapshot of aggregate {} {} not used: {}. Rebuilding it from its events.", aggregateType, aggregateId, whyUnusable);
    return null;
  }

  private String whySnapshotIsUnusable(String aggregateId, AggregateState snapshot) {
    if (snapshot == null) {
      return null;
    }
    if (!StringUtils.equals(snapshot.getAggregateId(), aggregateId)) {
      return "it belongs to aggregate " + snapshot.getAggregateId() + " instead of " + aggregateId;
    }
    // A type without payload is an unreadable aggregate (see SnapshotSerde), not a removal.
    if (snapshot.getPayload() == null && snapshot.getType() != null) {
      return "its aggregate can't be read, e.g. its class was moved or a field no longer fits";
    }
    String wrongPayload = whyPayloadDoesNotMatch(snapshot.getPayload());
    if (wrongPayload != null) {
      return wrongPayload;
    }
    // Also for a removed state: a later revision may not remove it.
    if (snapshot.getRevision() != revision) {
      return "it was made with revision " + snapshot.getRevision() + " of " + aggregateClass.getSimpleName()
          + ", the code is revision " + revision;
    }
    return null;
  }

  private AggregateState applyEvents(String aggregateId, AggregateState start, Iterator<Event> events) {
    String wrongStartPayload = whyPayloadDoesNotMatch(start.getPayload());
    if (wrongStartPayload != null) {
      throw new EventReplayException("The state before replay cannot be used: " + wrongStartPayload + ".");
    }
    AggregateState state = start;
    while (events.hasNext()) {
      Event event = events.next();
      if (event.getPayload() == null) {
        // A removed class must not silently drop out of the state: require an upcaster.
        throw new EventReplayException("Stored event " + event.getId() + " (" + event.getType() + ") cannot be replayed: its class no longer exists. Add an upcaster that renames it.");
      }
      if (!StringUtils.equals(event.getAggregateType(), aggregateType) || !StringUtils.equals(event.getAggregateId(), aggregateId)) {
        throw new EventReplayException("Stored event " + event.getId() + " (" + event.getType() + ") belongs to "
            + event.getAggregateType() + " " + event.getAggregateId() + ", but was read as " + aggregateType + " " + aggregateId + ".");
      }
      if (event.getSequence() != state.getVersion() + 1) {
        // A gap, repeat or reorder would give another state than the one that was handled.
        throw new EventReplayException("The stored events of aggregate " + event.getAggregateType() + " " + event.getAggregateId()
            + " are incomplete or out of order: expected #" + (state.getVersion() + 1) + ", found #" + event.getSequence()
            + " (" + event.getType() + ", event " + event.getId() + ").");
      }
      EventSourcingHandlerMethod handler = eventSourcingHandlers.get(event.getPayload().getClass());
      log.trace("Replaying event {} ({}) at sequence {}: handler {}", event.getType(), event.getAggregateId(),
          event.getSequence(), handler != null ? "found" : "absent, payload unchanged");
      Object payload = state.getPayload();
      if (handler != null) {
        payload = handler.handle(event, state);
        String wrongPayload = whyPayloadDoesNotMatch(payload);
        if (wrongPayload != null) {
          throw new EventReplayException("The state after stored event " + event.getId() + " (" + event.getType() + ") cannot be used: " + wrongPayload + ".");
        }
      }
      state = AggregateState.after(event, payload, revision);
    }
    return state;
  }

  /** Returns why a payload cannot be this aggregate state, or {@code null}. */
  private String whyPayloadDoesNotMatch(Object payload) {
    if (payload == null || payload.getClass() == aggregateClass) {
      return null;
    }
    return "its aggregate is " + payload.getClass().getName() + ", but " + aggregateType + " requires " + aggregateClass.getName();
  }
}
