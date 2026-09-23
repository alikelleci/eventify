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
 * Rebuilds aggregates from their stored snapshots and events. Select an aggregate type once with
 * {@link #forType(String)}; the resulting repository only needs aggregate identifiers. It only reads stores and
 * applies event sourcing handlers; command processing owns all writes.
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
  private final Map<String, AggregateDefinition> definitions;
  /** The aggregate this repository is scoped to; absent only before {@link #forType(String)} is called. */
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

  /** A repository for one aggregate type. All its operations address that type. */
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
    return aggregateType();
  }

  /** The current aggregate state: always present, with a null payload before creation or after removal. */
  public AggregateState replay(String aggregateId) {
    long started = System.nanoTime();
    String aggregateType = aggregateType();
    AggregateState snapshot = usableSnapshot(aggregateId);
    AggregateState start = snapshot != null ? snapshot : AggregateState.empty(aggregateId);
    try (EventStore.EventIterator events = eventStore.events(aggregateType, aggregateId, start.getVersion() + 1, Long.MAX_VALUE)) {
      AggregateState state = applyEvents(aggregateId, start, events, null);
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
  public AggregateState replay(String aggregateId, long untilSequence, ReplayListener listener) {
    String aggregateType = aggregateType();
    AggregateState storedSnapshot = snapshotStore.get(aggregateType, aggregateId);
    AggregateState snapshot = usableSnapshotOrNull(aggregateId, storedSnapshot);
    AggregateState start;
    if (snapshot != null && snapshot.getVersion() <= untilSequence) {
      if (listener == null || snapshot.getVersion() != untilSequence || !allEventsStored(aggregateId, storedSnapshot)) {
        start = snapshot;
      } else {
        start = AggregateState.empty(aggregateId);
      }
    } else {
      if (!allEventsStored(aggregateId, storedSnapshot)) {
        return null;
      }
      start = AggregateState.empty(aggregateId);
    }
    try (EventStore.EventIterator events = eventStore.events(aggregateType, aggregateId, start.getVersion() + 1, untilSequence)) {
      return applyEvents(aggregateId, start, events, listener);
    }
  }

  /** Applies newly produced events for this aggregate type in memory, before command processing stores them. */
  public AggregateState applyEvents(AggregateState state, List<Event> events) {
    return applyEvents(state.getAggregateId(), state, events.iterator(), null);
  }

  /** The stored event at a sequence. */
  public Event event(String aggregateId, long sequence) {
    return eventStore.get(aggregateType(), aggregateId, sequence);
  }

  /** Stored events, oldest first. The caller closes the iterator. */
  public EventStore.EventIterator events(String aggregateId) {
    return eventStore.events(aggregateType(), aggregateId);
  }

  /** Stored events, newest first. The caller closes the iterator. */
  public EventStore.EventIterator eventsNewestFirst(String aggregateId, long from, long to) {
    return eventStore.eventsNewestFirst(aggregateType(), aggregateId, from, to);
  }

  /** The usable snapshot, for history readers that need to show where its replay starts. */
  public AggregateState snapshot(String aggregateId) {
    String aggregateType = aggregateType();
    return usableSnapshotOrNull(aggregateId, snapshotStore.get(aggregateType, aggregateId));
  }

  /** Whether this aggregate should be snapshotted at its current version. */
  public boolean isSnapshotDue(long snapshotVersion, AggregateState state) {
    return definition().isSnapshotDue(snapshotVersion, state.getVersion());
  }

  /** Whether this aggregate deletes events before a snapshot. */
  public boolean deletesEventsAtSnapshot() {
    return definition().deletesEventsAtSnapshot();
  }

  /** Uses the already read snapshot to distinguish a new aggregate from pruned history without reading it again. */
  private boolean allEventsStored(String aggregateId, AggregateState storedSnapshot) {
    try (EventStore.EventIterator events = eventStore.events(aggregateType(), aggregateId)) {
      if (events.hasNext()) {
        return events.next().getSequence() == 1;
      }
      // A snapshot at a positive version proves there were events. An empty store then means history is missing.
      return storedSnapshot == null || storedSnapshot.getVersion() == 0;
    }
  }

  private AggregateState usableSnapshot(String aggregateId) {
    String aggregateType = aggregateType();
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

  /** A usable snapshot for optional readers; an unusable one simply means no known state there. */
  private AggregateState usableSnapshotOrNull(String aggregateId, AggregateState stored) {
    return whySnapshotIsOutdated(aggregateId, stored) == null ? stored : null;
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

  private AggregateState applyEvents(String aggregateId, AggregateState start, Iterator<Event> events, ReplayListener listener) {
    String aggregateType = aggregateType();
    String wrongStartPayload = definition().whyPayloadDoesNotMatch(start.getPayload());
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
        String wrongPayload = definition().whyPayloadDoesNotMatch(payload);
        if (wrongPayload != null) {
          throw new EventReplayException("The state after stored event " + event.getId() + " (" + event.getType() + ") cannot be used: " + wrongPayload + ".");
        }
      }
      state = AggregateState.after(event, payload, definition().revision());
    }
    return state;
  }

  private String aggregateType() {
    return definition().type();
  }

  private AggregateDefinition definition() {
    if (definition == null) {
      throw new IllegalStateException("Choose an aggregate type first: call AggregateRepository.forType(...).");
    }
    return definition;
  }

  private static Map<String, AggregateDefinition> definitionsOf(Collection<Class<?>> aggregateClasses) {
    return aggregateClasses.stream()
        .map(AggregateDefinition::new)
        .collect(Collectors.toUnmodifiableMap(AggregateDefinition::type, definition -> definition));
  }

  /** The fixed configuration of one aggregate type in this Eventify instance. */
  private static final class AggregateDefinition {
    private final String type;
    private final Class<?> aggregateClass;
    private final int revision;
    private final SnapshotPolicy snapshotPolicy;

    AggregateDefinition(Class<?> aggregateClass) {
      this.type = AggregateTypes.of(aggregateClass);
      this.aggregateClass = aggregateClass;
      this.revision = Revisions.of(aggregateClass);
      this.snapshotPolicy = SnapshotPolicy.of(aggregateClass);
    }

    String type() {
      return type;
    }

    int revision() {
      return revision;
    }

    boolean isSnapshotDue(long snapshotVersion, long aggregateVersion) {
      return snapshotPolicy.isSnapshotDue(snapshotVersion, aggregateVersion);
    }

    boolean deletesEventsAtSnapshot() {
      return snapshotPolicy.deleteEvents();
    }

    /** Why this snapshot cannot rebuild this aggregate type, or {@code null} when it can. */
    String whySnapshotIsOutdated(AggregateState snapshot) {
      // A removed state deliberately has no payload type. A type without a payload instead came from a snapshot whose
      // aggregate could not be deserialized and must not silently become a deletion.
      if (snapshot.getPayload() == null && snapshot.getType() != null) {
        return "its aggregate can't be read, e.g. its class was moved or a field no longer fits";
      }
      String wrongPayload = whyPayloadDoesNotMatch(snapshot.getPayload());
      if (wrongPayload != null) {
        return wrongPayload;
      }
      // Checked for a removed state too: a later revision of the event sourcing handlers may not remove it.
      int stored = snapshot.getRevision() == 0 ? 1 : snapshot.getRevision();
      if (stored != revision) {
        return "it was made with revision " + stored + " of " + aggregateClass.getSimpleName()
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
