package io.github.alikelleci.eventify.core.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.internal.EventHandlerMethod;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.serialization.internal.JsonRoundTrip;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;

/** Runs the event handlers and sends the commands they return. Here, not in the event package: it makes commands. */
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

    List<Object> commandPayloads = new ArrayList<>();
    handlers.eventHandlers(event.getPayload().getClass()).stream()
        .sorted(Comparator.comparingInt(EventHandlerMethod::getPriority).reversed())
        .peek(handler -> log.debug("Handling event: eventType={}, aggregateId={}, sequence={}, handler={}.{}", event.getType(), event.getAggregateId(), event.getSequence(), handler.getMethod().getDeclaringClass().getSimpleName(), handler.getMethod().getName()))
        .forEach(handler -> commandPayloads.addAll(handler.handle(event)));

    // All built after every handler ran: a failing handler or command sends none of them.
    Metadata metadata = event.getMetadata().with(CAUSATION_ID, event.getId());
    List<Command> commands = commandPayloads.stream()
        .map(payload -> Command.builder().payload(payload).metadata(metadata).build())
        // A command its handler can't read would be skipped there without a trace: fails here instead.
        .map(command -> JsonRoundTrip.copy(objectMapper, command, Command.class, "Command " + command.getType()))
        .toList();

    // A new record: the event's headers, such as the reply topic of its command, are not the new command's.
    commands.forEach(command -> {
      log.debug("Sending command: commandId={}, commandType={}, aggregateId={}, eventId={}", command.getId(), command.getType(), command.getAggregateId(), event.getId());
      context.forward(new Record<>(command.getAggregateId(), command, command.getTimestamp().toEpochMilli()));
    });
  }

  @Override
  public void close() {

  }
}
