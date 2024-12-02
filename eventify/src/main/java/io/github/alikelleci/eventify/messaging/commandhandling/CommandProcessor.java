package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;


@Slf4j
public class CommandProcessor implements FixedKeyProcessor<String, Command, CommandResult> {

  private final Eventify eventify;
  private FixedKeyProcessorContext<String, CommandResult> context;
  private KeyValueStore<String, AggregateState> snapshotStore;

  public CommandProcessor(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, CommandResult> context) {
    this.context = context;
    this.snapshotStore = context.getStateStore("snapshot-store");
  }

  @Override
  public void process(FixedKeyRecord<String, Command> fixedKeyRecord) {
    String key = fixedKeyRecord.key();
    Command command = fixedKeyRecord.value();

    try {
      // Load aggregate state
      AggregateState state = loadAggregate(key);

      // Execute command
      List<Event> events = executeCommand(state, command);

      // Return if no events
      if (CollectionUtils.isEmpty(events)) {
        return;
      }

      // Apply events
      for (Event event : events) {
        aggregate = applyEvent(aggregate, event);
      }

      // Save snapshot
      if (aggregate != null) {
        log.debug("Creating snapshot: {}", aggregate);
        saveSnapshot(aggregate);
      } else {
        deleteSnapshot(key);
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

  protected List<Event> executeCommand(AggregateState state, Command command) {
    CommandHandler commandHandler = eventify.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return new ArrayList<>();
    }

    return commandHandler.apply(state, command);
  }

  protected Aggregate loadAggregate(String aggregateId) {
    log.debug("Loading aggregate state...");
    Aggregate aggregate = loadFromSnapshot(aggregateId);

    log.debug("Current aggregate state: {}", aggregate);
    return aggregate;
  }

  protected Aggregate loadFromSnapshot(String aggregateId) {
    return snapshotStore.get(aggregateId);
  }

  protected Aggregate applyEvent(Aggregate aggregate, Event event) {
    EventSourcingHandler eventSourcingHandler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
    if (eventSourcingHandler != null) {
      aggregate = eventSourcingHandler.apply(aggregate, event);
    }
    return aggregate;
  }

  protected void saveSnapshot(Aggregate aggregate) {
    snapshotStore.put(aggregate.getAggregateId(), aggregate);
  }

  private void deleteSnapshot(String key) {
    snapshotStore.delete(key);
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
