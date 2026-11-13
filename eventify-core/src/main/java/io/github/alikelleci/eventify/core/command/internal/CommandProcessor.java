package io.github.alikelleci.eventify.core.command.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.SnapshotStore;
import io.github.alikelleci.eventify.core.aggregate.internal.DefaultAggregateRepository;
import io.github.alikelleci.eventify.core.aggregate.AggregateRepository.ReplayResult;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventStore;
import io.github.alikelleci.eventify.core.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.internal.StoreNames;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMismatchException;
import io.github.alikelleci.eventify.core.serialization.internal.JsonRoundTrip;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;

@Slf4j
public class CommandProcessor implements FixedKeyProcessor<String, Command, CommandResult> {

  /** Kafka's default for max.request.size. */
  private static final int DEFAULT_MAX_RECORD_SIZE = 1024 * 1024;
  /** Room for what Kafka adds to a record itself. */
  private static final int RECORD_OVERHEAD = 1024;

  private final HandlerRegistry handlers;
  private final ObjectMapper objectMapper;
  private FixedKeyProcessorContext<String, CommandResult> context;
  private int maxRecordSize;
  private EventStore eventStore;
  private SnapshotStore snapshotStore;
  private Map<String, DefaultAggregateRepository> repositories;

  public CommandProcessor(HandlerRegistry handlers, ObjectMapper objectMapper) {
    this.handlers = handlers;
    this.objectMapper = objectMapper;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, CommandResult> context) {
    this.context = context;
    Object configured = context.appConfigs().getOrDefault(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
        context.appConfigs().get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
    maxRecordSize = configured != null ? Integer.parseInt(configured.toString()) : DEFAULT_MAX_RECORD_SIZE;

    KeyValueStore<String, Event> events = context.getStateStore(StoreNames.EVENT_STORE);
    KeyValueStore<String, AggregateState> snapshots = context.getStateStore(StoreNames.SNAPSHOT_STORE);
    eventStore = new EventStore(events);
    snapshotStore = new SnapshotStore(snapshots);
    repositories = DefaultAggregateRepository.forTypes(eventStore, snapshotStore, handlers.eventSourcingHandlers(), handlers.aggregateClasses());
  }

  @Override
  public void process(FixedKeyRecord<String, Command> fixedKeyRecord) {
    String aggregateId = fixedKeyRecord.key();
    Command command = fixedKeyRecord.value();
    CommandHandlerMethod commandHandler = handlers.commandHandler(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("Skipping command, no handler registered: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId());
      return; // not a command of this application
    }
    if (!isWritable(objectMapper, command)) {
      return;
    }

    // Decide: nothing is written yet, so any exception rejects the command.
    DefaultAggregateRepository aggregateRepository = repositories.get(commandHandler.getAggregateType());
    ReplayResult replayResult;
    List<Event> events;
    AggregateState newState;
    try {
      // The state is loaded by record key: another key would load another aggregate.
      if (!StringUtils.equals(aggregateId, command.getAggregateId())) {
        throw new AggregateIdMismatchException("Command " + command.getType() + " has aggregate id " + command.getAggregateId() + ", but its record key is " + aggregateId + ".");
      }
      log.debug("Handling command: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId());
      replayResult = aggregateRepository.replay(aggregateId);
      events = copyEvents(toEvents(commandHandler.getAggregateType(), command, replayResult.currentState(), commandHandler.handle(command, replayResult.currentState())));
      newState = aggregateRepository.applyEvents(replayResult.currentState(), events);
      // The result holds the command and all its events: when it fits, every record of the command does.
      requireSendable("The result of command " + command.getType(), aggregateId, new CommandResult.Success(command, events), fixedKeyRecord.headers());
    } catch (Exception e) {
      logFailure(command, e);
      CommandResult failure = new CommandResult.Failure(command, ExceptionUtils.getRootCauseMessage(e));
      try {
        // The failure holds the command too: a command that is too large itself gets no result.
        requireSendable("The result of command " + command.getType(), aggregateId, failure, fixedKeyRecord.headers());
      } catch (RuntimeException tooLarge) {
        log.error("Skipping command result, it can't be sent: commandId={}, commandType={}, aggregateId={}, reason={}", command.getId(), command.getType(), command.getAggregateId(), tooLarge.getMessage());
        return;
      }
      context.forward(fixedKeyRecord.withValue(failure));
      return;
    }

    // Write: not caught. A failed write or send fails the task, and exactly-once rolls back all of it, the result too.
    eventStore.save(events);
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

  private void saveSnapshot(DefaultAggregateRepository aggregateRepository, AggregateState state) {
    AggregateState snapshot = copySnapshot(aggregateRepository, state);
    if (snapshot == null) {
      return;
    }
    log.debug("Saving snapshot: aggregateType={}, aggregateId={}, version={}", aggregateRepository.getAggregateType(), snapshot.getAggregateId(), snapshot.getVersion());
    snapshotStore.save(aggregateRepository.getAggregateType(), snapshot);
    if (aggregateRepository.deletesEventsAtSnapshot()) {
      long deleted = eventStore.deleteBefore(aggregateRepository.getAggregateType(), snapshot.getAggregateId(), snapshot.getVersion());
      log.debug("Deleted events before snapshot: aggregateType={}, aggregateId={}, snapshotVersion={}, deleted={}", aggregateRepository.getAggregateType(), snapshot.getAggregateId(), snapshot.getVersion(), deleted);
    }
  }

  /** A JSON copy of the state; {@code null} (no snapshot) when copying fails in any way: a snapshot is optional. */
  private AggregateState copySnapshot(DefaultAggregateRepository aggregateRepository, AggregateState state) {
    try {
      AggregateState snapshot = JsonRoundTrip.copy(objectMapper, state, AggregateState.class, "Snapshot " + aggregateRepository.getAggregateType());
      requireSendable("Snapshot " + aggregateRepository.getAggregateType(), state.getAggregateId(), snapshot, new RecordHeaders());
      return snapshot;
    } catch (RuntimeException e) {
      log.warn("Skipping snapshot: aggregateType={}, aggregateId={}, version={}, reason={}", aggregateRepository.getAggregateType(), state.getAggregateId(), state.getVersion(), e.getMessage());
      return null;
    }
  }

  /** Throws when the record can't be written as JSON or is over max.request.size: Kafka would only refuse it when sending, and that stops Eventify. */
  private void requireSendable(String name, String key, Object value, Headers headers) {
    long size = RECORD_OVERHEAD + key.getBytes(StandardCharsets.UTF_8).length;
    try {
      size += objectMapper.writeValueAsBytes(value).length;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(name + " cannot be written as JSON: " + e.getOriginalMessage(), e);
    }
    for (Header header : headers) {
      size += header.key().length() + (header.value() != null ? header.value().length : 0);
    }
    if (size > maxRecordSize) {
      throw new RecordTooLargeException(name + " is " + size + " bytes, more than Kafka accepts: max.request.size is " + maxRecordSize + ".");
    }
  }

  /** Every result holds the command: one that can't be written as JSON gets no result, instead of stopping Eventify. */
  static boolean isWritable(ObjectMapper objectMapper, Command command) {
    try {
      objectMapper.writeValueAsBytes(command);
      return true;
    } catch (JsonProcessingException e) {
      log.error("Skipping command, it can't be written as JSON: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId(), e);
      return false;
    }
  }

  private void logFailure(Command command, Exception e) {
    Throwable throwable = ExceptionUtils.getRootCause(e);
    String message = ExceptionUtils.getRootCauseMessage(e);

    if (throwable instanceof ValidationException) {
      log.debug("Command rejected, validation failed: commandId={}, commandType={}, aggregateId={}, reason={}", command.getId(), command.getType(), command.getAggregateId(), message, throwable);
    } else {
      log.error("Command failed: commandId={}, commandType={}, aggregateId={}, reason={}", command.getId(), command.getType(), command.getAggregateId(), message, throwable);
    }
  }
}
