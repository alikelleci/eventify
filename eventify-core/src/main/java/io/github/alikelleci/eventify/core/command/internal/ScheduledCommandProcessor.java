package io.github.alikelleci.eventify.core.command.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.internal.StoreKeys;
import io.github.alikelleci.eventify.core.internal.StoreNames;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Keeps scheduled commands until their delivery time, then sends them to their command topic. */
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
    Long deliverAt = deliverAt(record.headers());
    if (deliverAt == null) {
      log.warn("Skipping scheduled command, no valid delivery time: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId());
      return;
    }
    scheduled.put(StoreKeys.scheduled(deliverAt, command.getId()), command);
  }

  /** Removes the command a cancel record names; nothing when it is delivered already. */
  private void cancel(String aggregateId, Headers headers) {
    String commandId = text(headers, HeaderNames.CANCEL);
    Long deliverAt = deliverAt(headers);
    if (StringUtils.isBlank(commandId) || deliverAt == null) {
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
      log.debug("Delivering scheduled command: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId());
      context.forward(new Record<>(command.getAggregateId(), command, now));
      scheduled.delete(entry.key);
    }
  }

  /** The delivery time from the {@link HeaderNames#DELIVER_AT} header; {@code null} when missing or invalid. */
  private static Long deliverAt(Headers headers) {
    String text = text(headers, HeaderNames.DELIVER_AT);
    if (text == null) {
      return null;
    }
    try {
      long millis = Long.parseLong(text);
      return millis >= 0 ? millis : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String text(Headers headers, String name) {
    Header header = headers != null ? headers.lastHeader(name) : null;
    return header != null && header.value() != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
  }
}
