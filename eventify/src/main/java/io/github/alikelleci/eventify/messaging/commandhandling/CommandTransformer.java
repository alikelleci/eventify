package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
public class CommandTransformer implements ValueTransformerWithKey<String, Command, CommandResult> {

  private final Eventify eventify;
  private ProcessorContext context;

  public CommandTransformer(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public CommandResult transform(String key, Command command) {
    CommandHandler commandHandler = eventify.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      return null;
    }

    Class<?> aggregateType = commandHandler.getMethod().getParameters()[0].getType();

    // 1. Load aggregate state
    Aggregate aggregate = loadAggregate(key, aggregateType);

    // 2. Execute command
    CommandResult result = executeCommand(commandHandler, aggregate, command);

    if (result instanceof Success) {
      // 3. Save events
      for (Event event : ((Success) result).getEvents()) {
        saveEvent(event, aggregateType);
      }

      // 4. Save snapshot if needed
      Optional.ofNullable(aggregate)
          .filter(aggr -> aggr.getSnapshotTreshold() > 0)
          .filter(aggr -> aggr.getVersion() % aggr.getSnapshotTreshold() == 0)
          .ifPresent(aggr -> {
            log.debug("Creating snapshot: {}", aggr);
            saveSnapshot(aggr, aggregateType);

            // 5. Delete events after snapshot
            if (eventify.isDeleteEventsOnSnapshot()) {
              log.debug("Events prior to this snapshot will be deleted");
              deleteEvents(aggr, aggregateType);
            }
          });
    }

    return result;
  }

  @Override
  public void close() {

  }

  protected CommandResult executeCommand(CommandHandler commandHandler, Aggregate aggregate, Command command) {
    try {
      List<Event> events = commandHandler.apply(aggregate, command);
      return Success.builder()
          .command(command)
          .events(events)
          .build();

    } catch (Exception e) {
      return Failure.builder()
          .command(command)
          .cause(ExceptionUtils.getRootCauseMessage(e))
          .build();
    }
  }

  protected Aggregate loadAggregate(String aggregateId, Class<?> aggregateType) {
    AtomicLong sequence = new AtomicLong(0);
    AtomicLong counter = new AtomicLong(0);

    String from = aggregateId.concat("@");
    String to = aggregateId.concat("@z");

    Aggregate aggregate = loadFromSnapshot(aggregateId, aggregateType);
    if (aggregate != null) {
      log.debug("Snapshot found: {}", aggregate);
      from = aggregate.getEventId();
      sequence.set(aggregate.getVersion());
    }

    log.debug("Loading aggregate state by applying events...");

    try (KeyValueIterator<String, ValueAndTimestamp<Event>> iterator = getEventStore(aggregateType).range(from, to)) {
      while (iterator.hasNext()) {
        Event event = iterator.next().value.value();
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

    log.debug("Total events applied: {}", counter.get());
    log.debug("Current aggregate state reconstructed: {}", aggregate);
    return aggregate;
  }

  protected Aggregate loadFromSnapshot(String aggregateId, Class<?> aggregateType) {
    return Optional.ofNullable(getSnapshotStore(aggregateType).get(aggregateId))
        .map(ValueAndTimestamp::value)
        .orElse(null);
  }

  protected void saveEvent(Event event, Class<?> aggregateType) {
    getEventStore(aggregateType).putIfAbsent(event.getId(), ValueAndTimestamp.make(event, event.getTimestamp().toEpochMilli()));
  }

  protected void saveSnapshot(Aggregate aggregate, Class<?> aggregateType) {
    getSnapshotStore(aggregateType).put(aggregate.getAggregateId(), ValueAndTimestamp.make(aggregate, aggregate.getTimestamp().toEpochMilli()));
  }

  protected void deleteEvents(Aggregate aggregate, Class<?> aggregateType) {
    AtomicLong counter = new AtomicLong(0);

    String from = aggregate.getAggregateId().concat("@");
    String to = aggregate.getEventId();

    try (KeyValueIterator<String, ValueAndTimestamp<Event>> iterator = getEventStore(aggregateType).range(from, to)) {
      while (iterator.hasNext()) {
        Event event = iterator.next().value.value();
        getEventStore(aggregateType).delete(event.getId());
        counter.incrementAndGet();
      }
    }
    log.debug("Total events deleted: {}", counter.get());
  }

  private TimestampedKeyValueStore<String, Event> getEventStore(Class<?> aggregateType) {
    return context.getStateStore(aggregateType.getSimpleName() + "-event-store");
  }

  private TimestampedKeyValueStore<String, Aggregate> getSnapshotStore(Class<?> aggregateType) {
    return context.getStateStore(aggregateType.getSimpleName() + "-snapshot-store");
  }
}
