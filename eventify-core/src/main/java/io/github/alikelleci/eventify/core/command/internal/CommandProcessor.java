package io.github.alikelleci.eventify.core.command.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateReplayer;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.internal.ApplyEventMethod;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.internal.CommandResult.Failure;
import io.github.alikelleci.eventify.core.command.internal.CommandResult.Success;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.handler.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
import io.github.alikelleci.eventify.core.store.ReadOnlyEventStore;
import io.github.alikelleci.eventify.core.store.internal.EventStore;
import io.github.alikelleci.eventify.core.store.internal.SnapshotStore;
import io.github.alikelleci.eventify.core.store.internal.StoreNames;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


@Slf4j
public class CommandProcessor implements FixedKeyProcessor<String, Command, CommandResult> {

  private final HandlerRegistry handlers;
  private final ObjectMapper objectMapper;
  private final AggregateReplayer aggregateReplay;
  private FixedKeyProcessorContext<String, CommandResult> context;
  private EventStore eventStore;
  private SnapshotStore snapshotStore;

  public CommandProcessor(HandlerRegistry handlers, ObjectMapper objectMapper) {
    this.handlers = handlers;
    this.objectMapper = objectMapper;
    this.aggregateReplay = new AggregateReplayer(handlers.eventSourcingHandlers());
  }

  @Override
  public void init(FixedKeyProcessorContext<String, CommandResult> context) {
    this.context = context;
    this.eventStore = new EventStore(context.getStateStore(StoreNames.EVENT_STORE));
    this.snapshotStore = new SnapshotStore(context.getStateStore(StoreNames.SNAPSHOT_STORE));
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

    if (events == null) {
      return; // no command handler: not a command of this application
    }

    // Not caught: a failure from here on is not the command's, and must not be committed as its failure. It fails
    // the task, and exactly-once aborts the transaction with all that was written for this command.
    for (Event event : events) {
      saveEvent(event);
    }

    // Runs the topology after it on this call: the sends to the result and event topics. Also without events: the
    // command is accepted, and its caller waits for that answer.
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
   * is accepted. Empty when the command is accepted without events; {@code null} when there is no handler for it.
   */
  protected List<Event> executeCommand(String aggregateId, Command command) {
    CommandHandlerMethod commandHandler = handlers.commandHandler(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return null;
    }

    // The aggregate is loaded by the record key: another key would hand the handler the state of another aggregate.
    if (!StringUtils.equals(aggregateId, command.getAggregateId())) {
      throw new AggregateIdMismatchException("Record key does not match the aggregate identifier of command " + command.getType() + ". Expected " + command.getAggregateId() + ", but was " + aggregateId);
    }

    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());
    AggregateState state = loadAggregate(aggregateId);
    List<Event> events = eventStore.assignIds(aggregateId, commandHandler.apply(state, command));

    // Copied as the handler returned them, before anything else runs: an event may share objects with the aggregate
    // (e.g. its list of items), and applying the events below may change those. The copies are what is stored and sent.
    // Events are written as JSON when they are stored and sent, after the command is accepted, where a failure stops
    // the application. Copied through JSON now, an event that can't be written or read back fails this command instead.
    List<Event> copies = new ArrayList<>(events.size());
    for (Event event : events) {
      copies.add(copyThroughJson(event));
    }

    // Stored, an event is replayed at every load: one its event sourcing handler can't apply would make every next
    // command of this aggregate fail. Applied now, it fails this command instead, before it is stored. The copies are
    // applied, not the events as returned: a replay reads them as they are stored, after being written as JSON.
    applyEvents(state, copies);

    return copies;
  }

  private Event copyThroughJson(Event event) {
    byte[] json;
    try {
      json = objectMapper.writeValueAsBytes(event);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Event " + event.getType() + " cannot be written as JSON: " + e.getOriginalMessage(), e);
    }
    try {
      return objectMapper.readValue(json, Event.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Event " + event.getType() + " cannot be read back from JSON: " + e.getMessage(), e);
    }
  }

  private void applyEvents(AggregateState state, List<Event> events) {
    for (Event event : events) {
      ApplyEventMethod handler = handlers.eventSourcingHandler(event.getPayload().getClass());
      if (handler != null) {
        state = handler.apply(state, event);
      } else {
        log.debug("No Event Sourcing Handler found for event: {} ({}), state unchanged", event.getType(), event.getAggregateId());
        state = state != null ? state.after(event) : null;
      }
    }
  }

  protected AggregateState loadAggregate(String aggregateId) {
    Instant startTime = Instant.now();

    AggregateState snapshot = loadFromSnapshot(aggregateId);
    if (snapshot != null) {
      log.debug("Snapshot found: {} ({}) at version {}", snapshot.getType(), aggregateId, snapshot.getVersion());
    }

    log.debug("Loading aggregate state by applying events...");
    AggregateReplayer.Result replay;
    try (ReadOnlyEventStore.Events events = eventStore.events(aggregateId, snapshot != null ? snapshot.getEventId() : null, null)) {
      replay = aggregateReplay.replay(events, snapshot);
    }
    AggregateState state = replay.state();

    Instant endTime = Instant.now();
    Duration duration = Duration.between(startTime, endTime);

    log.debug("Number of events replayed: {}", replay.replayed());
    // Only ids, types and versions: the state and the payloads are application data, e.g. personal data.
    log.debug("Aggregate state reconstructed in {} ms: {} ({}) at version {}", duration.toMillis(),
        state != null ? state.getType() : null, aggregateId, state != null ? state.getVersion() : 0);

    // Save snapshot if needed: when the version passed a multiple of the threshold since the last snapshot. Not only when
    // it is exactly one: a command with several events can step over it.
    long startVersion = snapshot != null ? snapshot.getVersion() : 0;
    Optional.ofNullable(state)
        .filter(s -> s.getSnapshotThreshold() > 0)
        .filter(s -> s.getVersion() / s.getSnapshotThreshold() > startVersion / s.getSnapshotThreshold())
        .ifPresent(s -> {
          log.debug("Creating snapshot: {} ({}) at version {}", s.getType(), s.getAggregateId(), s.getVersion());
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
    eventStore.append(event);
  }

  protected void saveSnapshot(AggregateState state) {
    snapshotStore.save(state);
  }

  protected void deleteEvents(AggregateState state) {
    log.debug("Number of events deleted: {}", eventStore.deleteBefore(state));
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
