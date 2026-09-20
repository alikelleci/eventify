package io.github.alikelleci.eventify.core.command.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateReplayer;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.handler.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
import io.github.alikelleci.eventify.core.store.exception.EventStoreException;
import io.github.alikelleci.eventify.core.store.internal.AggregateRepository;
import io.github.alikelleci.eventify.core.store.internal.WritableEventStore;
import io.github.alikelleci.eventify.core.store.internal.WritableSnapshotStore;
import io.github.alikelleci.eventify.core.store.internal.StoreNames;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.util.List;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;

@Slf4j
public class CommandProcessor implements FixedKeyProcessor<String, Command, CommandResult> {

  private final HandlerRegistry handlers;
  private final ObjectMapper objectMapper;
  private final AggregateReplayer replayer;
  private FixedKeyProcessorContext<String, CommandResult> context;
  private AggregateRepository aggregates;

  public CommandProcessor(HandlerRegistry handlers, ObjectMapper objectMapper) {
    this.handlers = handlers;
    this.objectMapper = objectMapper;
    this.replayer = new AggregateReplayer(handlers.eventSourcingHandlers());
  }

  @Override
  public void init(FixedKeyProcessorContext<String, CommandResult> context) {
    this.context = context;
    this.aggregates = new AggregateRepository(replayer, objectMapper,
        new WritableEventStore(context.getStateStore(StoreNames.EVENT_STORE)),
        new WritableSnapshotStore(context.getStateStore(StoreNames.SNAPSHOT_STORE)));
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
      throw new AggregateIdMismatchException("Record key does not match the aggregate identifier of command " + command.getType() + ". Expected " + command.getAggregateId() + ", but was " + aggregateId);
    }

    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());
    AggregateState state = aggregates.load(aggregateId);
    List<Object> payloads = commandHandler.apply(state, command);
    // The events take over the command's metadata, and name it as their cause.
    return aggregates.record(aggregateId, state, payloads, command.getMetadata().with(CAUSATION_ID, command.getId()));
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
