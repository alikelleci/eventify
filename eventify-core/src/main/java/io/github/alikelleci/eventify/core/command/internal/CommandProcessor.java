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
import io.github.alikelleci.eventify.core.internal.ExceptionCauses;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
import io.github.alikelleci.eventify.core.store.exception.EventStoreException;
import io.github.alikelleci.eventify.core.store.EventStore;
import io.github.alikelleci.eventify.core.store.internal.StoreNames;
import io.github.alikelleci.eventify.core.serialization.internal.JsonRoundTrip;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;

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
    } catch (EventStoreException | TaskMigratedException e) {
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
    long snapshotVersion = snapshotVersion(aggregateType, aggregateId);
    boolean snapshotDue = isSnapshotDue(aggregateType, snapshotVersion, state);
    try {
      List<Event> events = copyEvents(events(aggregateType, command, state, commandHandler.handle(command, state)));
      AggregateState newState = repository.applyEvents(aggregateType, state, events);
      save(events, aggregateType, snapshotVersion, newState);
      return events;
    } catch (EventStoreException | TaskMigratedException e) {
      throw e;
    } catch (Exception e) {
      // A rejected command still leaves a checkpoint of the history that was successfully rebuilt for it.
      if (snapshotDue) {
        saveSnapshot(aggregateType, state);
      }
      throw e;
    }
  }

  /**
   * The command's events: they follow the history the command was handled on, so the first gets the sequence after the
   * version of that state. The state is immutable, so its version is still the one the handler was given.
   */
  private List<Event> events(String aggregateType, Command command, AggregateState state, List<Object> payloads) {
    long sequence = state.getVersion();
    Metadata metadata = command.getMetadata().with(CAUSATION_ID, command.getId());
    List<Event> events = new ArrayList<>(payloads.size());
    for (Object payload : payloads) {
      Event event = Event.builder()
          .aggregateType(aggregateType)
          .payload(payload)
          .metadata(metadata)
          .sequence(++sequence)
          .build();
      events.add(event);
    }
    return events;
  }

  /** JSON copies of the events: an event that can't be stored and read back rejects its command, before any writes. */
  private List<Event> copyEvents(List<Event> events) {
    return events.stream()
        .map(event -> JsonRoundTrip.copy(objectMapper, event, Event.class, "Event " + event.getType()))
        .toList();
  }

  /** Store writes belong to the processor's transaction; failures must escape command rejection and abort it. */
  private void save(List<Event> events, String aggregateType, long snapshotVersion, AggregateState state) {
    try {
      eventStore.save(events);
      saveSnapshotIfDue(aggregateType, snapshotVersion, state);
    } catch (TaskMigratedException e) {
      throw e; // fenced: Kafka Streams hands the task over, it must see this exception as is
    } catch (Exception e) {
      throw new EventStoreException("Could not save the command outcome for aggregate " + aggregateType + " "
          + state.getAggregateId() + ". " + ExceptionUtils.getRootCauseMessage(e), e);
    }
  }

  private void saveSnapshotIfDue(String aggregateType, long snapshotVersion, AggregateState state) {
    if (!isSnapshotDue(aggregateType, snapshotVersion, state)) {
      return;
    }
    saveSnapshot(aggregateType, state);
  }

  private void saveSnapshot(String aggregateType, AggregateState state) {
    AggregateState snapshot = copySnapshot(aggregateType, state);
    if (snapshot == null) {
      return;
    }
    try {
      log.debug("Creating snapshot: {} ({}) at version {}", aggregateType, snapshot.getAggregateId(), snapshot.getVersion());
      snapshotStore.save(aggregateType, snapshot);
      if (aggregateDefinitions.deletesEventsAtSnapshot(aggregateType)) {
        long deleted = eventStore.deleteBefore(aggregateType, snapshot.getAggregateId(), snapshot.getVersion());
        log.debug("Deleted {} events before snapshot: {} ({}) at version {}", deleted, aggregateType, snapshot.getAggregateId(), snapshot.getVersion());
      }
    } catch (TaskMigratedException e) {
      throw e;
    } catch (Exception e) {
      throw new EventStoreException("Could not save the snapshot of aggregate " + aggregateType + " "
          + snapshot.getAggregateId() + ". " + ExceptionUtils.getRootCauseMessage(e), e);
    }
  }

  /** A JSON copy of the state; {@code null} when it can't be written as JSON: then there is no snapshot. */
  private AggregateState copySnapshot(String aggregateType, AggregateState state) {
    try {
      return JsonRoundTrip.copy(objectMapper, state, AggregateState.class, "Snapshot " + aggregateType);
    } catch (IllegalArgumentException e) {
      log.warn("Snapshot of {} ({}) at version {} skipped: {}", aggregateType, state.getAggregateId(), state.getVersion(), e.getMessage());
      return null;
    }
  }

  /** The version of the aggregate's usable snapshot; 0 when it has none. Read once per command: nothing writes it meanwhile. */
  private long snapshotVersion(String aggregateType, String aggregateId) {
    AggregateState snapshot = repository.snapshot(aggregateType, aggregateId);
    return snapshot != null ? snapshot.getVersion() : 0;
  }

  private boolean isSnapshotDue(String aggregateType, long snapshotVersion, AggregateState state) {
    return aggregateDefinitions.isSnapshotDue(aggregateType, snapshotVersion, state.getVersion());
  }

  private void logFailure(Exception e) {
    Throwable throwable = ExceptionCauses.rootCauseOrSelf(e);
    String message = ExceptionUtils.getRootCauseMessage(e);

    if (throwable instanceof ValidationException) {
      log.debug("Handling command failed: {}", message, throwable);
    } else {
      log.error("Handling command failed: {}", message, throwable);
    }
  }
}
