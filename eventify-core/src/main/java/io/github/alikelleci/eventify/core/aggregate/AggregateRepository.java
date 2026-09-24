package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.aggregate.exception.EventReplayException;
import io.github.alikelleci.eventify.core.aggregate.exception.SnapshotOutdatedException;
import io.github.alikelleci.eventify.core.aggregate.internal.AggregateTypes;
import io.github.alikelleci.eventify.core.aggregate.internal.ApplyEventMethod;
import io.github.alikelleci.eventify.core.aggregate.internal.SnapshotPolicy;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.internal.Revisions;
import io.github.alikelleci.eventify.core.store.EventStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Rebuilds aggregates from their snapshots and events; read-only, command processing does all writes.
 * Every event advances the version, also one without a handler; events are applied in sequence order.
 */
@Slf4j
public final class AggregateRepository {

  /** The snapshot a replay started from ({@code null} when none was usable), the state after it, and how many events it applied. */
  public record ReplayResult(AggregateState usedSnapshot, AggregateState currentState, long eventsReplayed) {
  }

  private final EventStore eventStore;
  private final SnapshotStore snapshotStore;
  private final Map<Class<?>, ApplyEventMethod> applyMethods;
  private final Map<String, AggregateDefinition> definitions;
  private final AggregateDefinition definition;

  public AggregateRepository(EventStore eventStore, SnapshotStore snapshotStore, Map<Class<?>, ApplyEventMethod> applyMethods,
                             Collection<Class<?>> aggregateClasses) {
    this(eventStore, snapshotStore, applyMethods, definitionsOf(aggregateClasses), null);
  }

  private AggregateRepository(EventStore eventStore, SnapshotStore snapshotStore, Map<Class<?>, ApplyEventMethod> applyMethods,
                              Map<String, AggregateDefinition> definitions, AggregateDefinition definition) {
    this.eventStore = eventStore;
    this.snapshotStore = snapshotStore;
    this.applyMethods = applyMethods;
    this.definitions = definitions;
    this.definition = definition;
  }

  /** A repository for one aggregate type. */
  public AggregateRepository forType(String aggregateType) {
    AggregateDefinition definition = definitions.get(aggregateType);
    if (definition == null) {
      throw new IllegalArgumentException("This Eventify instance has no aggregate named '" + aggregateType
          + "'. It handles " + definitions.keySet() + ".");
    }
    return new AggregateRepository(eventStore, snapshotStore, applyMethods, definitions, definition);
  }

  /** The aggregate type selected with {@link #forType(String)}. */
  public String getAggregateType() {
    return definition().type();
  }

  /** The current aggregate state: always present, with a null payload before creation or after removal. */
  public ReplayResult replay(String aggregateId) {
    long started = System.nanoTime();
    String aggregateType = getAggregateType();
    AggregateState snapshot = usableSnapshot(aggregateId);
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

  /**
   * The state after the event at this sequence, 0 for before the first one; {@code null} when earlier events were
   * deleted and no usable snapshot covers it.
   */
  public AggregateState stateAt(String aggregateId, long sequence) {
    if (sequence < 0) {
      throw new IllegalArgumentException("Cannot give the state of aggregate " + aggregateId + " at sequence " + sequence + ": a sequence starts at 1.");
    }
    String aggregateType = getAggregateType();
    AggregateState storedSnapshot = snapshotStore.get(aggregateType, aggregateId);
    AggregateState start;
    if (storedSnapshot != null && storedSnapshot.getVersion() <= sequence && whySnapshotIsOutdated(aggregateId, storedSnapshot) == null) {
      start = storedSnapshot;
    } else if (allEventsStored(aggregateId, storedSnapshot)) {
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

  /** Applies newly produced events for this aggregate type in memory, before command processing stores them. */
  public AggregateState applyEvents(AggregateState state, List<Event> events) {
    return applyEvents(state.getAggregateId(), state, events.iterator());
  }

  /** The stored event at a sequence. */
  public Event event(String aggregateId, long sequence) {
    return eventStore.get(getAggregateType(), aggregateId, sequence);
  }

  /** Stored events, oldest first. The caller closes the iterator. */
  public EventStore.EventIterator events(String aggregateId) {
    return eventStore.events(getAggregateType(), aggregateId);
  }

  /** Stored events, newest first. The caller closes the iterator. */
  public EventStore.EventIterator eventsNewestFirst(String aggregateId, long from, long to) {
    return eventStore.eventsNewestFirst(getAggregateType(), aggregateId, from, to);
  }

  /** Whether this aggregate should be snapshotted at its current version. */
  public boolean isSnapshotDue(long snapshotVersion, AggregateState state) {
    return definition().snapshotPolicy().isSnapshotDue(snapshotVersion, state.getVersion());
  }

  /** Whether this aggregate deletes events before a snapshot. */
  public boolean deletesEventsAtSnapshot() {
    return definition().snapshotPolicy().deleteEvents();
  }

  /** Whether the history still starts at event 1: tells a new aggregate from pruned history. */
  private boolean allEventsStored(String aggregateId, AggregateState storedSnapshot) {
    try (EventStore.EventIterator events = eventStore.events(getAggregateType(), aggregateId)) {
      if (events.hasNext()) {
        return events.next().getSequence() == 1;
      }
      // No events but a snapshot past version 0: the history was pruned.
      return storedSnapshot == null || storedSnapshot.getVersion() == 0;
    }
  }

  private AggregateState usableSnapshot(String aggregateId) {
    String aggregateType = getAggregateType();
    AggregateState stored = snapshotStore.get(aggregateType, aggregateId);
    String whyOutdated = whySnapshotIsOutdated(aggregateId, stored);
    if (whyOutdated == null) {
      return stored;
    }
    if (!allEventsStored(aggregateId, stored)) {
      throw new SnapshotOutdatedException("The snapshot of aggregate " + aggregateType + " " + aggregateId + " can't be used: " + whyOutdated
          + ". The aggregate can't be rebuilt without it: the events before it were deleted (@EnableSnapshotting(deleteEvents = true)).");
    }
    log.info("Snapshot of aggregate {} {} not used: {}. Rebuilding it from its events.", aggregateType, aggregateId, whyOutdated);
    return null;
  }

  private String whySnapshotIsOutdated(String aggregateId, AggregateState snapshot) {
    if (snapshot == null) {
      return null;
    }
    if (!StringUtils.equals(snapshot.getAggregateId(), aggregateId)) {
      return "it belongs to aggregate " + snapshot.getAggregateId() + " instead of " + aggregateId;
    }
    return definition().whySnapshotIsOutdated(snapshot);
  }

  private AggregateState applyEvents(String aggregateId, AggregateState start, Iterator<Event> events) {
    String aggregateType = getAggregateType();
    String wrongStartPayload = definition().whyPayloadDoesNotMatch(start.getPayload());
    if (wrongStartPayload != null) {
      throw new EventReplayException("The state before replay cannot be used: " + wrongStartPayload + ".");
    }
    AggregateState state = start;
    while (events.hasNext()) {
      Event event = events.next();
      if (event.getPayload() == null) {
        // A removed class must not silently drop out of the state: require an upcaster.
        throw new EventReplayException("Stored event " + event.getId() + " (" + event.getType() + ") cannot be replayed: its class no longer exists. Add an upcaster that renames it to its current class.");
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
      ApplyEventMethod handler = applyMethods.get(event.getPayload().getClass());
      log.trace("Replaying event {} ({}) at sequence {}: handler {}", event.getType(), event.getAggregateId(),
          event.getSequence(), handler != null ? "found" : "absent, payload unchanged");
      Object payload = state.getPayload();
      if (handler != null) {
        payload = handler.apply(event, state);
        String wrongPayload = definition().whyPayloadDoesNotMatch(payload);
        if (wrongPayload != null) {
          throw new EventReplayException("The state after stored event " + event.getId() + " (" + event.getType() + ") cannot be used: " + wrongPayload + ".");
        }
      }
      state = AggregateState.after(event, payload, definition().revision());
    }
    return state;
  }

  private AggregateDefinition definition() {
    if (definition == null) {
      throw new IllegalStateException("Choose an aggregate type first: call AggregateRepository.forType(...).");
    }
    return definition;
  }

  private static Map<String, AggregateDefinition> definitionsOf(Collection<Class<?>> aggregateClasses) {
    return aggregateClasses.stream()
        .map(AggregateDefinition::of)
        .collect(Collectors.toUnmodifiableMap(AggregateDefinition::type, definition -> definition));
  }

  /** The fixed configuration of one aggregate type. */
  private record AggregateDefinition(String type, Class<?> aggregateClass, int revision, SnapshotPolicy snapshotPolicy) {

    static AggregateDefinition of(Class<?> aggregateClass) {
      return new AggregateDefinition(AggregateTypes.of(aggregateClass), aggregateClass, Revisions.of(aggregateClass),
          SnapshotPolicy.of(aggregateClass));
    }

    /** Why this snapshot cannot rebuild this aggregate type, or {@code null} when it can. */
    String whySnapshotIsOutdated(AggregateState snapshot) {
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

    /** Why a payload cannot be the state of this aggregate type, or {@code null} when it can. */
    String whyPayloadDoesNotMatch(Object payload) {
      if (payload == null) {
        return null;
      }
      if (payload.getClass() != aggregateClass) {
        return "its aggregate is " + payload.getClass().getName() + ", but " + type + " requires "
            + aggregateClass.getName();
      }
      return null;
    }
  }
}
