package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.aggregate.exception.EventReplayException;
import io.github.alikelleci.eventify.core.aggregate.exception.SnapshotOutdatedException;
import io.github.alikelleci.eventify.core.aggregate.internal.ApplyEventMethod;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.EventStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Rebuilds aggregates from their stored snapshots and events. It only reads stores and applies event sourcing
 * handlers; command processing owns all writes.
 *
 * <p>Every event advances the version, including events without a handler and events that remove the payload.
 * Events are applied in sequence order, independently of their timestamps.
 */
@Slf4j
public final class AggregateRepository {

  /** Called before an event is applied, for readers that need the state at a particular event. */
  @FunctionalInterface
  public interface ReplayListener {
    void beforeHandle(Event eventBefore, AggregateState stateBefore);
  }

  private final EventStore eventStore;
  private final SnapshotStore snapshotStore;
  private final Map<Class<?>, ApplyEventMethod> applyMethods;
  private final AggregateDefinitions definitions;

  public AggregateRepository(EventStore eventStore, SnapshotStore snapshotStore, Map<Class<?>, ApplyEventMethod> applyMethods,
                             AggregateDefinitions definitions) {
    this.eventStore = eventStore;
    this.snapshotStore = snapshotStore;
    this.applyMethods = applyMethods;
    this.definitions = definitions;
  }

  /** The current aggregate state: always present, with a null payload before creation or after removal. */
  public AggregateState replay(String aggregateType, String aggregateId) {
    long started = System.nanoTime();
    definitions.requireType(aggregateType);
    AggregateState snapshot = usableSnapshot(aggregateType, aggregateId);
    AggregateState start = snapshot != null ? snapshot : AggregateState.empty(aggregateId);
    try (EventStore.EventIterator events = eventStore.events(aggregateType, aggregateId, start.getVersion() + 1, Long.MAX_VALUE)) {
      AggregateState state = applyEvents(aggregateType, aggregateId, start, events, null);
      // Payloads are application data, possibly personal: log only type, id, versions and timings.
      log.debug("Replayed {} events for {} ({}) to version {} in {} ms", state.getVersion() - start.getVersion(),
          aggregateType, aggregateId, state.getVersion(), (System.nanoTime() - started) / 1_000_000);
      return state;
    }
  }

  /**
   * Rebuilds the state at a stored event for a reader. {@code null} means earlier events were deleted and no suitable
   * snapshot exists to reconstruct that point.
   * A listener sees each replayed event before its handler can mutate the state. When the requested event is also the
   * snapshot event, replay from the beginning if possible so its previous state is observed too. Otherwise the
   * snapshot supplies the resulting state without calling the listener for that event.
   */
  public AggregateState replay(String aggregateType, String aggregateId, long untilSequence, ReplayListener listener) {
    definitions.requireType(aggregateType);
    AggregateState storedSnapshot = snapshotStore.get(aggregateType, aggregateId);
    AggregateState snapshot = usableSnapshotOrNull(aggregateType, aggregateId, storedSnapshot);
    AggregateState start;
    if (snapshot != null && snapshot.getVersion() <= untilSequence) {
      if (listener == null || snapshot.getVersion() != untilSequence || !allEventsStored(aggregateType, aggregateId, storedSnapshot)) {
        start = snapshot;
      } else {
        start = AggregateState.empty(aggregateId);
      }
    } else {
      if (!allEventsStored(aggregateType, aggregateId, storedSnapshot)) {
        return null;
      }
      start = AggregateState.empty(aggregateId);
    }
    try (EventStore.EventIterator events = eventStore.events(aggregateType, aggregateId, start.getVersion() + 1, untilSequence)) {
      return applyEvents(aggregateType, aggregateId, start, events, listener);
    }
  }

  /** Applies newly produced events for this aggregate type in memory, before command processing stores them. */
  public AggregateState applyEvents(String aggregateType, AggregateState state, List<Event> events) {
    definitions.requireType(aggregateType);
    return applyEvents(aggregateType, state.getAggregateId(), state, events.iterator(), null);
  }

  /** The stored event at a sequence. */
  public Event event(String aggregateType, String aggregateId, long sequence) {
    definitions.requireType(aggregateType);
    return eventStore.get(aggregateType, aggregateId, sequence);
  }

  /** Stored events, oldest first. The caller closes the iterator. */
  public EventStore.EventIterator events(String aggregateType, String aggregateId) {
    definitions.requireType(aggregateType);
    return eventStore.events(aggregateType, aggregateId);
  }

  /** Stored events, newest first. The caller closes the iterator. */
  public EventStore.EventIterator eventsNewestFirst(String aggregateType, String aggregateId, long from, long to) {
    definitions.requireType(aggregateType);
    return eventStore.eventsNewestFirst(aggregateType, aggregateId, from, to);
  }

  /** The usable snapshot, for history readers that need to show where its replay starts. */
  public AggregateState snapshot(String aggregateType, String aggregateId) {
    definitions.requireType(aggregateType);
    AggregateState storedSnapshot = snapshotStore.get(aggregateType, aggregateId);
    return usableSnapshotOrNull(aggregateType, aggregateId, storedSnapshot);
  }

  /** Whether history begins at event one, or no history has ever been recorded according to either store. */
  public boolean allEventsStored(String aggregateType, String aggregateId) {
    definitions.requireType(aggregateType);
    return allEventsStored(aggregateType, aggregateId, snapshotStore.get(aggregateType, aggregateId));
  }

  /** Uses the already read snapshot to distinguish a new aggregate from pruned history without reading it again. */
  private boolean allEventsStored(String aggregateType, String aggregateId, AggregateState storedSnapshot) {
    try (EventStore.EventIterator events = eventStore.events(aggregateType, aggregateId)) {
      if (events.hasNext()) {
        return events.next().getSequence() == 1;
      }
      // A snapshot at a positive version proves there were events. An empty store then means history is missing.
      return storedSnapshot == null || storedSnapshot.getVersion() == 0;
    }
  }

  private AggregateState usableSnapshot(String aggregateType, String aggregateId) {
    AggregateState stored = snapshotStore.get(aggregateType, aggregateId);
    String whyOutdated = whySnapshotIsOutdated(aggregateType, aggregateId, stored);
    if (whyOutdated == null) {
      return stored;
    }
    if (!allEventsStored(aggregateType, aggregateId, stored)) {
      throw new SnapshotOutdatedException("The snapshot of aggregate " + aggregateType + " " + aggregateId + " can't be used: " + whyOutdated
          + ". The aggregate can't be rebuilt without it: the events before it were deleted (@EnableSnapshotting(deleteEvents = true)).");
    }
    log.info("Snapshot of aggregate {} {} not used: {}. Rebuilding it from its events.", aggregateType, aggregateId, whyOutdated);
    return null;
  }

  /** A usable snapshot for optional readers; an unusable one simply means no known state there. */
  private AggregateState usableSnapshotOrNull(String aggregateType, String aggregateId, AggregateState stored) {
    return whySnapshotIsOutdated(aggregateType, aggregateId, stored) == null ? stored : null;
  }

  private String whySnapshotIsOutdated(String aggregateType, String aggregateId, AggregateState snapshot) {
    if (snapshot == null) {
      return null;
    }
    if (!StringUtils.equals(snapshot.getAggregateId(), aggregateId)) {
      return "it belongs to aggregate " + snapshot.getAggregateId() + " instead of " + aggregateId;
    }
    return definitions.whySnapshotIsOutdated(aggregateType, snapshot);
  }

  private AggregateState applyEvents(String aggregateType, String aggregateId, AggregateState start, Iterator<Event> events,
                                     ReplayListener listener) {
    String wrongStartPayload = definitions.whyPayloadDoesNotMatch(aggregateType, start.getPayload());
    if (wrongStartPayload != null) {
      throw new EventReplayException("The state before replay cannot be used: " + wrongStartPayload + ".");
    }
    AggregateState state = start;
    while (events.hasNext()) {
      Event event = events.next();
      if (event.getPayload() == null) {
        // A renamed/removed event class must not silently disappear from the reconstructed state: require upcasting.
        throw new EventReplayException("Stored event " + event.getId() + " (" + event.getType() + ") cannot be replayed: its class no longer exists. Add an upcaster that renames it to its current class.");
      }
      if (!StringUtils.equals(event.getAggregateType(), aggregateType) || !StringUtils.equals(event.getAggregateId(), aggregateId)) {
        throw new EventReplayException("Stored event " + event.getId() + " (" + event.getType() + ") belongs to "
            + event.getAggregateType() + " " + event.getAggregateId() + ", but was read as " + aggregateType + " " + aggregateId + ".");
      }
      if (event.getSequence() != state.getVersion() + 1) {
        // Missing, repeated or out-of-order events would produce a different state than the history that was handled.
        throw new EventReplayException("The stored events of aggregate " + event.getAggregateType() + " " + event.getAggregateId()
            + " are incomplete or out of order: expected #" + (state.getVersion() + 1) + ", found #" + event.getSequence()
            + " (" + event.getType() + ", event " + event.getId() + ").");
      }
      if (listener != null) {
        listener.beforeHandle(event, state);
      }
      ApplyEventMethod handler = applyMethods.get(event.getPayload().getClass());
      log.trace("Replaying event {} ({}) at sequence {}: handler {}", event.getType(), event.getAggregateId(),
          event.getSequence(), handler != null ? "found" : "absent, payload unchanged");
      Object payload = state.getPayload();
      if (handler != null) {
        payload = handler.apply(event, state);
        String wrongPayload = definitions.whyPayloadDoesNotMatch(aggregateType, payload);
        if (wrongPayload != null) {
          throw new EventReplayException("The state after stored event " + event.getId() + " (" + event.getType() + ") cannot be used: " + wrongPayload + ".");
        }
      }
      state = AggregateState.after(event, payload, definitions.revisionOf(aggregateType));
    }
    return state;
  }
}
