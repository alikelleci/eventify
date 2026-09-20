package io.github.alikelleci.eventify.core.command.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateDefinitions;
import io.github.alikelleci.eventify.core.aggregate.AggregateRepository;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.SnapshotStore;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.handler.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
import io.github.alikelleci.eventify.core.store.exception.EventStoreException;
import io.github.alikelleci.eventify.core.store.EventStore;
import io.github.alikelleci.eventify.core.store.internal.StoreNames;
import io.github.alikelleci.eventify.core.serialization.internal.JsonRoundTrip;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

@Slf4j
public class CommandProcessor implements FixedKeyProcessor<String, Command, CommandResult> {

  private final HandlerRegistry handlers;
  private final ObjectMapper objectMapper;
  private FixedKeyProcessorContext<String, CommandResult> context;
  private EventStore eventStore;
  private SnapshotStore snapshotStore;
  private AggregateRepository repository;
  private AggregateDefinitions aggregateDefinitions;

  public CommandProcessor(HandlerRegistry handlers, ObjectMapper objectMapper) {
    this.handlers = handlers;
    this.objectMapper = objectMapper;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, CommandResult> context) {
    this.context = context;

    KeyValueStore<String, Event> events = context.getStateStore(StoreNames.EVENT_STORE);
    KeyValueStore<String, AggregateState> snapshots = context.getStateStore(StoreNames.SNAPSHOT_STORE);
    eventStore = new EventStore(events);
    snapshotStore = new SnapshotStore(snapshots);
    aggregateDefinitions = new AggregateDefinitions(handlers.aggregateClasses());
    repository = new AggregateRepository(eventStore, snapshotStore, handlers.eventSourcingHandlers(), aggregateDefinitions);
  }

  @Override
  public void process(FixedKeyRecord<String, Command> fixedKeyRecord) {
    String key = fixedKeyRecord.key();
    Command command = fixedKeyRecord.value();

    List<Event> events;
    try {
      events = handleCommand(key, command);
    } catch (EventStoreException e) {
      // Not the command's failure, and it must not be committed as one: it fails the task, and exactly-once aborts
      // the transaction with all that was written for this command.
      throw e;
    } catch (Exception e) {
      // Everything that can reject the command, user code included: the aggregate is left as it was.
      logFailure(e);

      context.forward(fixedKeyRecord.withValue(new CommandResult.Failure(command, ExceptionUtils.getRootCauseMessage(e))));
      return;
    }

    if (events == null) {
      return; // no command handler: not a command of this application
    }

    // Runs the topology after it on this call: the sends to the result and event topics. Also without events: the
    // command is accepted, and its caller waits for that answer.
    context.forward(fixedKeyRecord.withValue(new CommandResult.Success(command, events)));
  }

  /**
   * Handles the command and records its events. Empty when the command is accepted without events; {@code null} when
   * there is no handler for it.
   */
  private List<Event> handleCommand(String aggregateId, Command command) {
    CommandHandlerMethod commandHandler = handlers.commandHandler(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return null;
    }

    // The aggregate is loaded by the record key: another key would hand the handler the state of another aggregate.
    if (!StringUtils.equals(aggregateId, command.getAggregateId())) {
      throw new AggregateIdMismatchException("Aggregate identifier does not match for command " + command.getType() + ". Expected " + command.getAggregateId() + ", but was " + aggregateId);
    }

    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());
    String aggregateType = commandHandler.getAggregateType();
    AggregateState state = repository.replay(aggregateType, aggregateId);
    AggregateState checkpoint = checkpointBeforeCommand(aggregateType, state);
    try {
      List<Event> events = copyEvents(commandHandler.handle(command, state));
      AggregateState newState = repository.applyEvents(state, events);
      save(events, aggregateType, newState);
      return events;
    } catch (EventStoreException e) {
      throw e;
    } catch (Exception e) {
      // A rejected command still leaves a checkpoint of the history that was successfully rebuilt for it.
      if (checkpoint != null) {
        saveSnapshotIfDue(aggregateType, checkpoint);
      }
      throw e;
    }
  }

  /**
   * Capture a due checkpoint before user code can mutate the aggregate, also during validation of produced events.
   * Only the failure path uses this copy; a successful command snapshots its resulting state. JSON uses the same
   * representation as the snapshot store, and a failure preparing that snapshot must abort the transaction too.
   */
  private AggregateState checkpointBeforeCommand(String aggregateType, AggregateState state) {
    try {
      return isSnapshotDue(aggregateType, state)
          ? JsonRoundTrip.copy(objectMapper, state, AggregateState.class, "Snapshot " + aggregateType)
          : null;
    } catch (Exception e) {
      throw new EventStoreException("Could not prepare the snapshot of aggregate " + aggregateType + " "
          + state.getAggregateId() + ". " + ExceptionUtils.getRootCauseMessage(e), e);
    }
  }

  /**
   * Copy before applying events: their payloads may share mutable objects with the aggregate. These copies are stored
   * and sent. The JSON round trip also rejects an event that cannot be persisted and replayed before any writes.
   */
  private List<Event> copyEvents(List<Event> events) {
    return events.stream()
        .map(event -> JsonRoundTrip.copy(objectMapper, event, Event.class, "Event " + event.getType()))
        .toList();
  }

  /** Store writes belong to the processor's transaction; failures must escape command rejection and abort it. */
  private void save(List<Event> events, String aggregateType, AggregateState state) {
    try {
      eventStore.save(events);
      saveSnapshotIfDue(aggregateType, state);
    } catch (Exception e) {
      throw new EventStoreException("Could not save the command outcome for aggregate " + aggregateType + " "
          + state.getAggregateId() + ". " + ExceptionUtils.getRootCauseMessage(e), e);
    }
  }

  private void saveSnapshotIfDue(String aggregateType, AggregateState state) {
    try {
      if (!isSnapshotDue(aggregateType, state)) {
        return;
      }
      log.debug("Creating snapshot: {} ({}) at version {}", aggregateType, state.getAggregateId(), state.getVersion());
      snapshotStore.save(aggregateType, state);
      if (aggregateDefinitions.deletesEventsAtSnapshot(aggregateType)) {
        long deleted = eventStore.deleteBefore(aggregateType, state.getAggregateId(), state.getVersion());
        log.debug("Deleted {} events before snapshot: {} ({}) at version {}", deleted, aggregateType, state.getAggregateId(), state.getVersion());
      }
    } catch (EventStoreException e) {
      throw e;
    } catch (Exception e) {
      throw new EventStoreException("Could not save the snapshot of aggregate " + aggregateType + " "
          + state.getAggregateId() + ". " + ExceptionUtils.getRootCauseMessage(e), e);
    }
  }

  private boolean isSnapshotDue(String aggregateType, AggregateState state) {
    AggregateState snapshot = repository.snapshot(aggregateType, state.getAggregateId());
    long snapshotVersion = snapshot != null ? snapshot.getVersion() : 0;
    return aggregateDefinitions.isSnapshotDue(aggregateType, snapshotVersion, state.getVersion());
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
