package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateReplayer;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.exception.SnapshotOutdatedException;
import io.github.alikelleci.eventify.core.aggregate.internal.SnapshotPolicy;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.ReadOnlyEventStore;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * The aggregates as the event store and the snapshot store keep them: loaded from their latest snapshot and the events
 * after it, snapshotted as their {@link SnapshotPolicy} says.
 */
@Slf4j
public class AggregateRepository {

  private final AggregateReplayer replayer;
  private final EventStore eventStore;
  private final SnapshotStore snapshotStore;

  public AggregateRepository(AggregateReplayer replayer, EventStore eventStore, SnapshotStore snapshotStore) {
    this.replayer = replayer;
    this.eventStore = eventStore;
    this.snapshotStore = snapshotStore;
  }

  /**
   * The current state of the aggregate; {@code null} when it has none. Saves a snapshot when one is due, and deletes
   * the events before it when the aggregate asks for that.
   */
  public AggregateState load(String aggregateId) {
    Instant startTime = Instant.now();

    AggregateState snapshot = usableSnapshot(aggregateId);
    if (snapshot != null) {
      log.debug("Snapshot found: {} ({}) at version {}", snapshot.getType(), aggregateId, snapshot.getVersion());
    }

    log.debug("Loading aggregate state by replaying events...");
    AggregateReplayer.Result replay;
    try (ReadOnlyEventStore.Events events = eventStore.events(aggregateId, snapshot != null ? snapshot.getVersion() + 1 : 1, Long.MAX_VALUE)) {
      replay = replayer.replay(events, snapshot);
    }
    AggregateState state = replay.state();

    Duration duration = Duration.between(startTime, Instant.now());
    log.debug("Number of events replayed: {}", replay.replayed());
    // Only ids, types and versions: the state and the payloads are application data, e.g. personal data.
    log.debug("Aggregate state reconstructed in {} ms: {} ({}) at version {}", duration.toMillis(),
        state != null ? state.getType() : null, aggregateId, state != null ? state.getVersion() : 0);

    if (state != null) {
      SnapshotPolicy policy = SnapshotPolicy.of(state.getPayload().getClass());
      if (policy.isDue(snapshot != null ? snapshot.getVersion() : 0, state.getVersion())) {
        log.debug("Creating snapshot: {} ({}) at version {}", state.getType(), state.getAggregateId(), state.getVersion());
        snapshotStore.save(state);
        if (policy.deleteEvents()) {
          log.debug("Events prior to this snapshot will be deleted");
          log.debug("Number of events deleted: {}", eventStore.deleteBefore(state));
        }
      }
    }

    return state;
  }

  /**
   * The aggregate's snapshot, when it can be used. An outdated one (see {@link SnapshotStore#whyOutdated}) is left out:
   * the aggregate is rebuilt from all its events, and snapshotted again. That can't be done when the events before the
   * snapshot were deleted: then the command fails, instead of going on with a state the current code would not compute.
   */
  private AggregateState usableSnapshot(String aggregateId) {
    AggregateState snapshot = snapshotStore.find(aggregateId);
    String whyOutdated = snapshot != null ? SnapshotStore.whyOutdated(snapshot) : null;
    if (whyOutdated == null) {
      return snapshot;
    }
    if (eventsBeforeWereDeleted(aggregateId, snapshot)) {
      throw new SnapshotOutdatedException("The snapshot of aggregate " + aggregateId + " can't be used: " + whyOutdated
          + ". The aggregate can't be rebuilt without it: the events before it were deleted (@EnableSnapshotting(deleteEvents = true)).");
    }
    log.info("Snapshot of aggregate {} not used: {}. Rebuilding it from its events.", aggregateId, whyOutdated);
    return null;
  }

  /**
   * Whether events before the snapshot's event are gone. Deleting them keeps the snapshot's event, so the aggregate's
   * first stored event then comes after its first sequence.
   */
  private boolean eventsBeforeWereDeleted(String aggregateId, AggregateState snapshot) {
    if (snapshot.getVersion() <= 1) {
      return false;
    }
    try (ReadOnlyEventStore.Events events = eventStore.events(aggregateId)) {
      return !events.hasNext() || events.next().getSequence() > 1;
    }
  }

  /** The events with the sequences after the aggregate's last stored event: see {@link EventStore#sequence}. */
  public List<Event> sequence(String aggregateId, List<Event> events) {
    return eventStore.sequence(aggregateId, events);
  }

  public void save(List<Event> events) {
    events.forEach(eventStore::append);
  }
}
