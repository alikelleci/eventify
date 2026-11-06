package io.github.alikelleci.eventify.core.command.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.internal.StoreKeys;
import io.github.alikelleci.eventify.core.internal.StoreNames;
import io.github.alikelleci.eventify.core.message.internal.Topics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Keeps scheduled commands until their delivery time, then sends them to their command topic; a cancel record removes one. */
@Slf4j
public class ScheduledCommandProcessor implements Processor<String, Command, String, Command> {

  /** How often due commands are delivered: a command is delivered up to this long after its time. */
  static final Duration INTERVAL = Duration.ofSeconds(1);

  /** At most this many per round, so a backlog can't make a transaction too large to commit. */
  static final int MAX_PER_ROUND = 1000;

  private final HandlerRegistry handlers;
  private final ObjectMapper objectMapper;
  private ProcessorContext<String, Command> context;
  private KeyValueStore<String, Command> scheduled;

  public ScheduledCommandProcessor(HandlerRegistry handlers, ObjectMapper objectMapper) {
    this.handlers = handlers;
    this.objectMapper = objectMapper;
  }

  @Override
  public void init(ProcessorContext<String, Command> context) {
    this.context = context;
    this.scheduled = context.getStateStore(StoreNames.SCHEDULED_COMMANDS);
    context.schedule(INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::deliverDue);
  }

  @Override
  public void process(Record<String, Command> record) {
    Command command = record.value();
    if (command == null) {
      cancel(record.key(), record.headers());
      return;
    }
    if (command.getPayload() == null) {
      return;
    }
    if (handlers.commandHandler(command.getPayload().getClass()) == null) {
      log.debug("Skipping scheduled command, no handler registered: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId());
      return; // another application delivers it
    }
    if (!CommandProcessor.isWritable(objectMapper, command)) {
      return;
    }
    Long deliverAt = ScheduleHeaders.deliverAt(record.headers());
    if (deliverAt == null) {
      log.warn("Skipping scheduled command, no valid delivery time: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId());
      return;
    }
    scheduled.put(StoreKeys.scheduled(deliverAt, command.getId()), command);
  }

  /** For the store: a stored command that can't be read anymore is read as {@code null}, so it can be skipped. */
  public static Deserializer<Command> nullWhenUnreadable(Deserializer<Command> deserializer) {
    return (topic, bytes) -> {
      try {
        return deserializer.deserialize(topic, bytes);
      } catch (SerializationException e) {
        return null;
      }
    };
  }

  /** Removes the command a cancel record names; nothing when it is delivered already. */
  private void cancel(String aggregateId, Headers headers) {
    String commandId = ScheduleHeaders.cancel(headers);
    Long deliverAt = ScheduleHeaders.deliverAt(headers);
    if (commandId == null || deliverAt == null) {
      return; // not a cancel record: a record without a value
    }
    log.debug("Cancelling scheduled command: commandId={}, aggregateId={}", commandId, aggregateId);
    scheduled.delete(StoreKeys.scheduled(deliverAt, commandId));
  }

  /** Delivers and removes the oldest due commands; the rest waits for the next round. */
  private void deliverDue(long now) {
    List<KeyValue<String, Command>> due = new ArrayList<>();
    try (KeyValueIterator<String, Command> iterator = scheduled.range(null, StoreKeys.scheduledUpTo(now))) {
      while (iterator.hasNext() && due.size() < MAX_PER_ROUND) {
        due.add(iterator.next());
      }
    }
    for (KeyValue<String, Command> entry : due) {
      Command command = entry.value;
      // Its class changed or is gone since it was scheduled: delivering it would stop Eventify, again after every restart.
      if (command == null || command.getPayload() == null || Topics.of(command.getPayload().getClass()) == null) {
        log.warn("Skipping scheduled command, it can't be delivered anymore: commandId={}", entry.key.substring(entry.key.indexOf(StoreKeys.SEPARATOR) + 1));
        scheduled.delete(entry.key);
        continue;
      }
      if (!CommandProcessor.isWritable(objectMapper, command)) {
        scheduled.delete(entry.key);
        continue;
      }
      log.debug("Delivering scheduled command: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId());
      context.forward(new Record<>(command.getAggregateId(), command, now));
      scheduled.delete(entry.key);
    }
  }
}
