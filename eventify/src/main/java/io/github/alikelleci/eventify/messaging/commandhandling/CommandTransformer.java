package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.List;
import java.util.Optional;


@Slf4j
public class CommandTransformer implements ValueTransformerWithKey<String, Command, CommandResult> {

  private final Eventify eventify;
  private TimestampedKeyValueStore<String, Aggregate> snapshotStore;

  public CommandTransformer(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.snapshotStore = processorContext.getStateStore("snapshot-store");
  }

  @Override
  public CommandResult transform(String key, Command command) {
    CommandHandler commandHandler = eventify.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      return null;
    }

    // 1. Load aggregate state
    Aggregate aggregate = loadAggregate(key);

    // 2. Validate command against aggregate
    CommandResult result = commandHandler.apply(command, aggregate);

    if (result instanceof Success) {
      // 3. Save snapshot
      saveSnapshot(aggregate, ((Success) result).getEvents());
    }

    return result;
  }

  @Override
  public void close() {

  }

  protected Aggregate loadAggregate(String aggregateId) {
    return Optional.ofNullable(snapshotStore.get(aggregateId))
        .map(ValueAndTimestamp::value)
        .orElse(null);
  }

  protected void saveSnapshot(Aggregate aggregate, List<Event> events) {
    for (Event event : events) {
      EventSourcingHandler eventSourcingHandler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
      if (eventSourcingHandler != null) {
        aggregate = eventSourcingHandler.apply(event, aggregate);
      }
    }

    if (aggregate != null) {
      snapshotStore.put(aggregate.getAggregateId(), ValueAndTimestamp.make(aggregate, aggregate.getTimestamp().toEpochMilli()));
    }
  }

}
