package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
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
public class CommandProcessor implements FixedKeyProcessor<String, Command, CommandResult> {

  private final Eventify eventify;
  private FixedKeyProcessorContext<String, CommandResult> context;
  private KeyValueStore<String, Event> eventStore;
  private KeyValueStore<String, Aggregate> snapshotStore;

  public CommandProcessor(Eventify eventify) {
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
      // Load aggregate state
      Aggregate aggregate = loadAggregate(key);

      // Execute command
      List<Event> events = executeCommand(aggregate, command);

      // Return if no events
      if (CollectionUtils.isEmpty(events)) {
        return;
      }

      // Save events
      for (Event event : events) {
        saveEvent(event);
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

  protected List<Event> executeCommand(Aggregate aggregate, Command command) {
    CommandHandler commandHandler = eventify.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return new ArrayList<>();
    }

    return commandHandler.apply(aggregate, command);
  }

  protected Aggregate loadAggregate(String aggregateId) {
    Instant startTime = Instant.now();

    AtomicLong sequence = new AtomicLong(0);
    AtomicLong counter = new AtomicLong(0);

    String from = aggregateId.concat("@");
    String to = aggregateId.concat("@~");

    Aggregate aggregate = loadFromSnapshot(aggregateId);
    if (aggregate != null) {
      log.debug("Snapshot found: {}", aggregate);
      from = aggregate.getEventId();
      sequence.set(aggregate.getVersion());
    }

    log.debug("Loading aggregate state by applying events...");

    try (KeyValueIterator<String, Event> iterator = eventStore.range(from, to)) {
      while (iterator.hasNext()) {
        Event event = iterator.next().value;
        if (aggregate == null || !aggregate.getEventId().equals(event.getId())) {
          EventSourcingHandler eventSourcingHandler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
          if (eventSourcingHandler != null) {
            aggregate = eventSourcingHandler.apply(aggregate, event);

            sequence.incrementAndGet();
            counter.incrementAndGet();
          }
        }
      }
    }

    aggregate = Optional.ofNullable(aggregate)
        .map(aggr -> Aggregate.builder()
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
    log.debug("Aggregate state reconstructed: {}", aggregate);
    log.debug("Aggregate state reconstructed in: {} ms ({} sec)", duration.toMillis(), duration.toSeconds());

    // Save snapshot if needed
    Optional.ofNullable(aggregate)
        .filter(aggr -> counter.get() > 0)
        .filter(aggr -> aggr.getSnapshotTreshold() > 0)
        .filter(aggr -> aggr.getVersion() % aggr.getSnapshotTreshold() == 0)
        .ifPresent(aggr -> {
          log.debug("Creating snapshot: {}", aggr);
          saveSnapshot(aggr);

          // Delete events after snapshot
          if (aggr.deleteEvents()) {
            log.debug("Events prior to this snapshot will be deleted");
            deleteEvents(aggr);
          }
        });

    return aggregate;
  }

  protected Aggregate loadFromSnapshot(String aggregateId) {
    return snapshotStore.get(aggregateId);
  }

  protected void saveEvent(Event event) {
    eventStore.putIfAbsent(event.getId(), event);
  }

  protected void saveSnapshot(Aggregate aggregate) {
    snapshotStore.put(aggregate.getAggregateId(), aggregate);
  }

  protected void deleteEvents(Aggregate aggregate) {
    AtomicLong counter = new AtomicLong(0);

    String from = aggregate.getAggregateId().concat("@");
    String to = aggregate.getEventId();

    try (KeyValueIterator<String, Event> iterator = eventStore.range(from, to)) {
      while (iterator.hasNext()) {
        Event event = iterator.next().value;
        eventStore.delete(event.getId());
        counter.incrementAndGet();
      }
    }
    log.debug("Total events deleted: {}", counter.get());
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
