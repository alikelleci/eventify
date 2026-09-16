package io.github.alikelleci.eventify.core.messaging.commandhandling;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.common.exceptions.AggregateIdMismatchException;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateReplay;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import io.github.alikelleci.eventify.core.util.IdUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
public class CommandProcessor implements FixedKeyProcessor<String, Command, CommandResult> {

  private final Eventify eventify;
  private final AggregateReplay aggregateReplay;
  private FixedKeyProcessorContext<String, CommandResult> context;
  private KeyValueStore<String, Event> eventStore;
  private KeyValueStore<String, AggregateState> snapshotStore;

  public CommandProcessor(Eventify eventify) {
    this.eventify = eventify;
    this.aggregateReplay = new AggregateReplay(eventify.getEventSourcingHandlers());
  }

  @Override
  public void init(FixedKeyProcessorContext<String, CommandResult> context) {
    this.context = context;
    this.eventStore = context.getStateStore("event-store");
    this.snapshotStore = context.getStateStore("snapshot-store");
  }

  @Override
  public void process(FixedKeyRecord<String, Command> fixedKeyRecord) {
    String key = fixedKeyRecord.key();
    Command command = fixedKeyRecord.value();

    // Everything that can reject the command, user code included. Nothing of the command is stored yet: a failure
    // leaves the aggregate as it was.
    List<Event> events;
    try {
      events = executeCommand(key, command);
    } catch (Exception e) {
      logFailure(e);

      context.forward(fixedKeyRecord.withValue(Failure.builder()
          .command(command)
          .cause(ExceptionUtils.getRootCauseMessage(e))
          .build()));
      return;
    }

    if (CollectionUtils.isEmpty(events)) {
      return;
    }

    // Not caught: a failure from here on is not the command's, and must not be committed as its failure. It fails
    // the task, and exactly-once aborts the transaction with all that was written for this command.
    for (Event event : events) {
      saveEvent(event);
    }

    // Runs the topology after it on this call: the sends to the result and event topics.
    context.forward(fixedKeyRecord.withValue(Success.builder()
        .command(command)
        .events(events)
        .build()));
  }

  @Override
  public void close() {

  }

  /**
   * Handles the command and returns its events, without storing them: {@link #process} stores them once the command
   * is accepted.
   */
  protected List<Event> executeCommand(String aggregateId, Command command) {
    CommandHandler commandHandler = eventify.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return new ArrayList<>();
    }

    // The aggregate is loaded by the record key: another key would hand the handler the state of another aggregate.
    if (!StringUtils.equals(aggregateId, command.getAggregateId())) {
      throw new AggregateIdMismatchException("Record key does not match the aggregate identifier of command " + command.getType() + ". Expected " + command.getAggregateId() + ", but was " + aggregateId);
    }

    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());
    AggregateState state = loadAggregate(aggregateId);
    List<Event> events = inOrder(aggregateId, commandHandler.apply(state, command));

    // Stored, an event is replayed at every load: one its event sourcing handler can't apply would make every next
    // command of this aggregate fail. Applied now, it fails this command instead, before it is stored.
    applyEvents(state, events);

    return events;
  }

  private void applyEvents(AggregateState state, List<Event> events) {
    for (Event event : events) {
      EventSourcingHandler handler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
      if (handler != null) {
        state = handler.apply(state, event);
      }
    }
  }

  /**
   * The events under keys after the aggregate's last stored event, in the order the handler returned them. The store
   * order is the replay order: it must be the order the events were handled in, not the order of the commands'
   * timestamps or of the clocks of the hosts that handled them.
   */
  private List<Event> inOrder(String aggregateId, List<Event> events) {
    String lastKey = lastEventKey(aggregateId);
    List<Event> ordered = new ArrayList<>(events.size());
    for (Event event : events) {
      Event keyed = event.withId(IdUtils.nextEventKey(aggregateId, lastKey));
      ordered.add(keyed);
      lastKey = keyed.getId();
    }
    return ordered;
  }

  /**
   * The key of the aggregate's last stored event; {@code null} when it has none. Deleting events at a snapshot keeps
   * the snapshot's event and the ones after it, so the last key is never deleted.
   */
  private String lastEventKey(String aggregateId) {
    try (KeyValueIterator<String, Event> iterator = eventStore.reverseRange(IdUtils.firstKey(aggregateId), IdUtils.lastKey(aggregateId))) {
      while (iterator.hasNext()) {
        String key = iterator.next().key;
        if (IdUtils.isKeyOf(aggregateId, key)) {
          return key;
        }
      }
    }
    return null;
  }

  protected AggregateState loadAggregate(String aggregateId) {
    Instant startTime = Instant.now();

    AggregateState snapshot = loadFromSnapshot(aggregateId);
    if (snapshot != null) {
      log.debug("Snapshot found: {}", snapshot);
    }

    log.debug("Loading aggregate state by applying events...");
    AggregateReplay.Result replay = aggregateReplay.replay(eventStore, aggregateId, snapshot, null);
    AggregateState state = replay.state();

    Instant endTime = Instant.now();
    Duration duration = Duration.between(startTime, endTime);

    log.debug("Number of events applied: {}", replay.applied());
    log.debug("Aggregate state reconstructed in {} ms ({} sec): {}", duration.toMillis(), duration.toSeconds(), state);

    // Save snapshot if needed: when the version passed a multiple of the threshold since the last snapshot. Not only when
    // it is exactly one: a command with several events can step over it.
    long startVersion = snapshot != null ? snapshot.getVersion() : 0;
    Optional.ofNullable(state)
        .filter(s -> s.getSnapshotThreshold() > 0)
        .filter(s -> s.getVersion() / s.getSnapshotThreshold() > startVersion / s.getSnapshotThreshold())
        .ifPresent(s -> {
          log.debug("Creating snapshot: {}", s);
          saveSnapshot(s);

          // Delete events after snapshot
          if (s.deleteEvents()) {
            log.debug("Events prior to this snapshot will be deleted");
            deleteEvents(s);
          }
        });

    return state;
  }

  protected AggregateState loadFromSnapshot(String aggregateId) {
    return snapshotStore.get(aggregateId);
  }

  protected void saveEvent(Event event) {
    eventStore.putIfAbsent(event.getId(), event);
  }

  protected void saveSnapshot(AggregateState state) {
    snapshotStore.put(state.getAggregateId(), state);
  }

  protected void deleteEvents(AggregateState state) {
    AtomicLong counter = new AtomicLong(0);

    String from = IdUtils.firstKey(state.getAggregateId());
    String to = state.getEventId();

    try (KeyValueIterator<String, Event> iterator = eventStore.range(from, to)) {
      while (iterator.hasNext()) {
        KeyValue<String, Event> entry = iterator.next();
        if (entry.key.equals(to)) {
          break; // keep the snapshot event itself
        }
        if (!IdUtils.isKeyOf(state.getAggregateId(), entry.key)) {
          continue; // another aggregate's event in the range: never ours to delete
        }
        eventStore.delete(entry.key);
        counter.incrementAndGet();
      }
    }
    log.debug("Number of events deleted: {}", counter.get());
  }

  private void logFailure(Exception e) {
    Throwable throwable = ExceptionUtils.getRootCause(e);
    String message = ExceptionUtils.getRootCauseMessage(e);

    if (throwable instanceof ValidationException) {
      log.debug("Handling command failed: {}", message, throwable);
    } else {
      log.error("Handling command failed: {}", message, throwable);
    }
  }
}
