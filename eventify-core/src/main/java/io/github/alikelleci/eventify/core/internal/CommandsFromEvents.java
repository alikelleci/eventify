package io.github.alikelleci.eventify.core.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.internal.HandledEvent;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.serialization.internal.JsonRoundTrip;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.List;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;

/** Turns the payloads event handlers return into commands, keyed by their own aggregate id. */
public class CommandsFromEvents implements Processor<String, HandledEvent, String, Command> {

  private final ObjectMapper objectMapper;
  private ProcessorContext<String, Command> context;

  public CommandsFromEvents(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public void init(ProcessorContext<String, Command> context) {
    this.context = context;
  }

  @Override
  public void process(Record<String, HandledEvent> record) {
    Event event = record.value().event();
    Metadata metadata = event.getMetadata().with(CAUSATION_ID, event.getId());

    // All built first: one that can't be sent sends none of them.
    List<Command> commands = record.value().commandPayloads().stream()
        .map(payload -> Command.builder().payload(payload).metadata(metadata).build())
        // A command its handler can't read would be skipped there without a trace: fails here instead.
        .map(command -> JsonRoundTrip.copy(objectMapper, command, Command.class, "Command " + command.getType()))
        .toList();

    // A new record: the event's headers, such as the reply topic of its command, are not the new command's.
    commands.forEach(command ->
        context.forward(new Record<>(command.getAggregateId(), command, command.getTimestamp().toEpochMilli())));
  }
}
