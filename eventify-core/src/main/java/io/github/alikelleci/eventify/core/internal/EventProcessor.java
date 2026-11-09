package io.github.alikelleci.eventify.core.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.Scheduled;
import io.github.alikelleci.eventify.core.command.internal.ScheduleHeaders;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.exception.EventHandlingException;
import io.github.alikelleci.eventify.core.event.internal.EventHandlerMethod;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.exception.TopicMissingException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import io.github.alikelleci.eventify.core.message.internal.Topics;
import io.github.alikelleci.eventify.core.serialization.internal.JsonRoundTrip;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;

/**
 * Runs the event handlers and sends the commands they return: a payload, a {@link Command} for its metadata, or a
 * {@link Scheduled} one. Here, not in the event package: it makes commands.
 */
@Slf4j
public class EventProcessor implements Processor<String, Event, String, Command> {

  private final HandlerRegistry handlers;
  private final ObjectMapper objectMapper;
  private ProcessorContext<String, Command> context;

  public EventProcessor(HandlerRegistry handlers, ObjectMapper objectMapper) {
    this.handlers = handlers;
    this.objectMapper = objectMapper;
  }

  @Override
  public void init(ProcessorContext<String, Command> context) {
    this.context = context;
  }

  @Override
  public void process(Record<String, Event> record) {
    Event event = record.value();

    List<Object> returned = new ArrayList<>();
    handlers.eventHandlers(event.getPayload().getClass()).stream()
        .sorted(Comparator.comparingInt(EventHandlerMethod::getPriority).reversed())
        .peek(handler -> log.debug("Handling event: eventType={}, aggregateId={}, sequence={}, handler={}.{}", event.getType(), event.getAggregateId(), event.getSequence(), handler.getMethod().getDeclaringClass().getSimpleName(), handler.getMethod().getName()))
        .forEach(handler -> returned.addAll(handler.handle(event)));

    // All built after every handler ran: a failing handler or command sends none of them.
    List<Outgoing> outgoing = returned.stream().map(value -> outgoing(event, value)).toList();

    // A new record: the event's headers, such as the reply topic of its command, are not the new command's.
    outgoing.forEach(out -> {
      Command command = out.command();
      Record<String, Command> commandRecord = new Record<>(command.getAggregateId(), command, command.getTimestamp().toEpochMilli());
      if (out.deliverAt() == null) {
        log.debug("Sending command: commandId={}, commandType={}, aggregateId={}, eventId={}", command.getId(), command.getType(), command.getAggregateId(), event.getId());
      } else {
        log.debug("Scheduling command: commandId={}, commandType={}, aggregateId={}, eventId={}, deliverAt={}", command.getId(), command.getType(), command.getAggregateId(), event.getId(), out.deliverAt());
        // The sink sends it to the scheduled topic.
        commandRecord = commandRecord.withHeaders(new RecordHeaders().add(ScheduleHeaders.deliverAt(out.deliverAt())));
      }
      context.forward(commandRecord);
    });
  }

  /** The command for one returned value, with the event's metadata and the handler's own keys. */
  private Outgoing outgoing(Event event, Object value) {
    Instant deliverAt = null;
    if (value instanceof Scheduled scheduled) {
      deliverAt = scheduled.deliverAt();
      value = scheduled.command();
    }
    Object payload = value instanceof Command command ? command.getPayload() : value;
    Metadata own = value instanceof Command command ? command.getMetadata() : Metadata.EMPTY;

    // Checked now: the topic and key are only looked up when sending. Failing like the handler did.
    try {
      if (Topics.of(payload.getClass()) == null) {
        throw new TopicMissingException("Command " + payload.getClass().getSimpleName() + " has no @Topic.");
      }
      Topic.validate(Topics.of(payload.getClass()).value());
      AggregateIdResolver.getAggregateId(payload);
    } catch (RuntimeException e) {
      throw new EventHandlingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }

    // The handler adds or replaces application keys; the flow and the cause stay the event's.
    Metadata metadata = event.getMetadata().with(applicationKeys(own)).with(CAUSATION_ID, event.getId());
    Command command = Command.builder().payload(payload).metadata(metadata).build();
    // A command its handler can't read would be skipped there without a trace: fails here instead.
    return new Outgoing(JsonRoundTrip.copy(objectMapper, command, Command.class, "Command " + command.getType()), deliverAt);
  }

  /** Without Eventify's own "$" keys. */
  private static Map<String, String> applicationKeys(Metadata metadata) {
    Map<String, String> keys = new HashMap<>();
    metadata.forEach((key, value) -> {
      if (!key.startsWith("$")) {
        keys.put(key, value);
      }
    });
    return keys;
  }

  private record Outgoing(Command command, Instant deliverAt) {
  }

  @Override
  public void close() {

  }
}
