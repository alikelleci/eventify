package io.github.alikelleci.eventify.core.command.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateRepository;
import io.github.alikelleci.eventify.core.aggregate.AggregateRepository.ReplayResult;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.SnapshotStore;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.handler.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
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
    repository = new AggregateRepository(eventStore, snapshotStore, handlers.eventSourcingHandlers(), handlers.aggregateClasses());
  }

  @Override
  public void process(FixedKeyRecord<String, Command> fixedKeyRecord) {
    String aggregateId = fixedKeyRecord.key();
    Command command = fixedKeyRecord.value();
    CommandHandlerMethod commandHandler = handlers.commandHandler(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return; // not a command of this application
    }

    // Decide: nothing is written yet, so any exception rejects the command.
    AggregateRepository aggregateRepository = repository.forType(commandHandler.getAggregateType());
    ReplayResult replayResult;
    List<Event> events;
    AggregateState newState;
    try {
      // The state is loaded by record key: another key would load another aggregate.
      if (!StringUtils.equals(aggregateId, command.getAggregateId())) {
        throw new AggregateIdMismatchException("Aggregate identifier does not match for command " + command.getType() + ". Expected " + command.getAggregateId() + ", but was " + aggregateId);
      }
      log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());
      replayResult = aggregateRepository.replay(aggregateId);
      events = copyEvents(toEvents(commandHandler.getAggregateType(), command, replayResult.currentState(), commandHandler.handle(command, replayResult.currentState())));
      newState = aggregateRepository.applyEvents(replayResult.currentState(), events);
    } catch (Exception e) {
      logFailure(e);
      context.forward(fixedKeyRecord.withValue(new CommandResult.Failure(command, ExceptionUtils.getRootCauseMessage(e))));
      return;
    }

    // Write: not caught. A failed write or send fails the task, and exactly-once rolls back all of it, the result too.
    eventStore.save(events);
    // Save snapshot if threshold is reached
    long snapshotVersion = replayResult.usedSnapshot() != null ? replayResult.usedSnapshot().getVersion() : 0;
    if (aggregateRepository.isSnapshotDue(snapshotVersion, newState)) {
      saveSnapshot(aggregateRepository, newState);
    }
    // Also without events: the command is accepted, and its sender waits for that answer.
    context.forward(fixedKeyRecord.withValue(new CommandResult.Success(command, events)));
  }

  /** The command's events, numbered on from the version of the state the handler was given. */
  private List<Event> toEvents(String aggregateType, Command command, AggregateState state, List<Object> payloads) {
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

  /** JSON copies: an event that can't round-trip rejects its command before any write. */
  private List<Event> copyEvents(List<Event> events) {
    return events.stream()
        .map(event -> JsonRoundTrip.copy(objectMapper, event, Event.class, "Event " + event.getType()))
        .toList();
  }

  private void saveSnapshot(AggregateRepository aggregateRepository, AggregateState state) {
    AggregateState snapshot = copySnapshot(aggregateRepository, state);
    if (snapshot == null) {
      return;
    }
    log.debug("Creating snapshot: {} ({}) at version {}", aggregateRepository.getAggregateType(), snapshot.getAggregateId(), snapshot.getVersion());
    snapshotStore.save(aggregateRepository.getAggregateType(), snapshot);
    if (aggregateRepository.deletesEventsAtSnapshot()) {
      long deleted = eventStore.deleteBefore(aggregateRepository.getAggregateType(), snapshot.getAggregateId(), snapshot.getVersion());
      log.debug("Deleted {} events before snapshot: {} ({}) at version {}", deleted, aggregateRepository.getAggregateType(), snapshot.getAggregateId(), snapshot.getVersion());
    }
  }

  /** A JSON copy of the state; {@code null} (no snapshot) when copying fails in any way: a snapshot is optional. */
  private AggregateState copySnapshot(AggregateRepository aggregateRepository, AggregateState state) {
    try {
      return JsonRoundTrip.copy(objectMapper, state, AggregateState.class, "Snapshot " + aggregateRepository.getAggregateType());
    } catch (RuntimeException e) {
      log.warn("Snapshot of {} ({}) at version {} skipped: {}", aggregateRepository.getAggregateType(), state.getAggregateId(), state.getVersion(), e.getMessage());
      return null;
    }
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
