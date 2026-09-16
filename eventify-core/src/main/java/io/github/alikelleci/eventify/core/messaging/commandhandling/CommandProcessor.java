package io.github.alikelleci.eventify.core.messaging.commandhandling;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateReplay;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
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

    try {
      // Execute command
      List<Event> events = executeCommand(key, command);

      // Return if no events
      if (CollectionUtils.isEmpty(events)) {
        return;
      }

      // Forward success
      context.forward(fixedKeyRecord.withValue(Success.builder()
          .command(command)
          .events(events)
          .build()));

    } catch (Exception e) {
      // Log failure
      logFailure(e);

      // Forward failure
      context.forward(fixedKeyRecord.withValue(Failure.builder()
          .command(command)
          .cause(ExceptionUtils.getRootCauseMessage(e))
          .build()));
    }
  }

  @Override
  public void close() {

  }

  protected List<Event> executeCommand(String aggregateId, Command command) {
    CommandHandler commandHandler = eventify.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return new ArrayList<>();
    }

    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());
    AggregateState state = loadAggregate(aggregateId);
    List<Event> events = commandHandler.apply(state, command);

    // Save events
    for (Event event : events) {
      saveEvent(event);
    }

    return events;
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

    // Save snapshot if needed
    Optional.ofNullable(state)
        .filter(s -> replay.applied() > 0)
        .filter(s -> s.getSnapshotThreshold() > 0)
        .filter(s -> s.getVersion() % s.getSnapshotThreshold() == 0)
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
