package io.github.alikelleci.eventify.core.store.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateReplayer;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.exception.SnapshotOutdatedException;
import io.github.alikelleci.eventify.core.aggregate.internal.SnapshotPolicy;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.serialization.internal.JsonRoundTrip;
import io.github.alikelleci.eventify.core.store.EventStore;
import io.github.alikelleci.eventify.core.store.exception.EventStoreException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The aggregates as the event store and the snapshot store keep them: loaded from their latest snapshot and the events
 * after it, recorded when a command changes them, and snapshotted as their {@link SnapshotPolicy} says.
 */
@Slf4j
public class AggregateRepository {

  private final AggregateReplayer replayer;
  private final ObjectMapper objectMapper;
  private final WritableEventStore eventStore;
  private final WritableSnapshotStore snapshotStore;

  public AggregateRepository(AggregateReplayer replayer, ObjectMapper objectMapper, WritableEventStore eventStore, WritableSnapshotStore snapshotStore) {
    this.replayer = replayer;
    this.objectMapper = objectMapper;
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
    try (EventStore.Events events = eventStore.events(aggregateId, snapshot != null ? snapshot.getVersion() + 1 : 1, Long.MAX_VALUE)) {
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
      if (policy.isSnapshotDue(snapshot != null ? snapshot.getVersion() : 0, state.getVersion())) {
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
   * The aggregate's snapshot, when it can be used. An outdated one (see {@link ReadableSnapshotStore#whyOutdated}) is left out:
   * the aggregate is rebuilt from all its events, and snapshotted again. That can't be done when the events before the
   * snapshot were deleted: then the command fails, instead of going on with a state the current code would not compute.
   */
  private AggregateState usableSnapshot(String aggregateId) {
    AggregateState snapshot = snapshotStore.get(aggregateId);
    String whyOutdated = snapshot != null ? ReadableSnapshotStore.whyOutdated(snapshot) : null;
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
    try (EventStore.Events events = eventStore.events(aggregateId)) {
      return !events.hasNext() || events.next().getSequence() > 1;
    }
  }

  /**
   * Records what happened to an aggregate: the payloads become its next events, in the order they are given, and are
   * stored.
   *
   * <p>An event is made with the place it has in its aggregate: the sequence after the last stored one. That is the
   * order the events were handled in, whatever the clocks of the hosts that handled them.
   *
   * @param state    the state the events happened to: they are applied to it before they are stored
   * @param metadata what every one of these events carries, e.g. the command they come from
   * @return the events as they are stored and sent
   */
  public List<Event> record(String aggregateId, AggregateState state, List<Object> payloads, Metadata metadata) {
    if (payloads.isEmpty()) {
      // A command that changes nothing, e.g. a handler that returns nothing: it is accepted, and the store is not read.
      return List.of();
    }

    long sequence = eventStore.lastSequence(aggregateId);

    // Copied as they were given, before anything else runs: a payload may share objects with the aggregate (e.g. its
    // list of items), and applying the events below may change those. The copies are what is stored and sent.
    // Events are written as JSON when they are stored and sent, where a failure stops the application. Copied through
    // JSON now, an event that can't be written or read back fails the command that produced it instead.
    List<Event> events = new ArrayList<>(payloads.size());
    for (Object payload : payloads) {
      Event event = Event.builder()
          .payload(payload)
          .metadata(metadata)
          .sequence(++sequence)
          .build();
      events.add(JsonRoundTrip.copy(objectMapper, event, Event.class, "Event " + event.getType()));
    }

    // Stored, an event is replayed at every load: one its event sourcing handler can't apply would make every next
    // command of this aggregate fail. Applied now, it fails the command that produced it, before it is stored.
    AggregateState applied = state;
    for (Event event : events) {
      applied = replayer.apply(applied, event);
    }

    store(aggregateId, events);
    return events;
  }

  /**
   * Stores the events of one recording. They are stored together or not at all: a failure is answered with an
   * {@link EventStoreException}, which command handling lets through instead of answering the command with it. Told as
   * a failure of the command, the events stored before it would stay behind without ever being sent.
   */
  private void store(String aggregateId, List<Event> events) {
    try {
      events.forEach(eventStore::append);
    } catch (Exception e) {
      throw new EventStoreException("Not all events of aggregate " + aggregateId + " could be stored: "
          + ExceptionUtils.getRootCauseMessage(e), e);
    }
  }
}
