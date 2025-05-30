package io.github.alikelleci.eventify.engine.kafka;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandProcessor;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
public class KafkaCommandProcessor implements CommandProcessor, FixedKeyProcessor<String, Command, CommandResult> {

  private final Eventify eventify;
  private FixedKeyProcessorContext<String, CommandResult> context;
  private KeyValueStore<String, Event> eventStore;
  private KeyValueStore<String, AggregateState> snapshotStore;

  public KafkaCommandProcessor(Eventify eventify) {
    this.eventify = eventify;
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

    AtomicLong sequence = new AtomicLong(0);
    AtomicLong counter = new AtomicLong(0);

    String from = aggregateId.concat("@");
    String to = aggregateId.concat("@~");

    AggregateState state = loadFromSnapshot(aggregateId);
    if (state != null) {
      log.debug("Snapshot found: {}", state);
      from = state.getEventId();
      sequence.set(state.getVersion());
    }

    log.debug("Loading aggregate state by applying events...");

    try (KeyValueIterator<String, Event> iterator = eventStore.range(from, to)) {
      while (iterator.hasNext()) {
        Event event = iterator.next().value;
        if (state == null || !state.getEventId().equals(event.getId())) {
          EventSourcingHandler eventSourcingHandler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
          if (eventSourcingHandler != null) {
            log.trace("Applying event: {} ({})", event.getType(), event.getAggregateId());
            state = eventSourcingHandler.apply(state, event);

            sequence.incrementAndGet();
            counter.incrementAndGet();
          }
        }
      }
    }

    state = Optional.ofNullable(state)
        .map(aggr -> AggregateState.builder()
            .timestamp(aggr.getTimestamp())
            .payload(aggr.getPayload())
            .metadata(aggr.getMetadata())
            .eventId(aggr.getEventId())
            .version(sequence.get())
            .build())
        .orElse(null);

    Instant endTime = Instant.now();
    Duration duration = Duration.between(startTime, endTime);

    log.debug("Number of events applied: {}", counter.get());
    log.debug("Aggregate state reconstructed in {} ms ({} sec): {}", duration.toMillis(), duration.toSeconds(), state);

    // Save snapshot if needed
    Optional.ofNullable(state)
        .filter(s -> counter.get() > 0)
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

    String from = state.getAggregateId().concat("@");
    String to = state.getEventId();

    try (KeyValueIterator<String, Event> iterator = eventStore.range(from, to)) {
      while (iterator.hasNext()) {
        Event event = iterator.next().value;
        eventStore.delete(event.getId());
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
