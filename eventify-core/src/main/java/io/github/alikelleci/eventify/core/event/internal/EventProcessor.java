package io.github.alikelleci.eventify.core.event.internal;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.internal.HandlerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Slf4j
public class EventProcessor implements FixedKeyProcessor<String, Event, HandledEvent> {

  private final HandlerRegistry handlers;
  private FixedKeyProcessorContext<String, HandledEvent> context;

  public EventProcessor(HandlerRegistry handlers) {
    this.handlers = handlers;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, HandledEvent> context) {
    this.context = context;
  }

  @Override
  public void process(FixedKeyRecord<String, Event> fixedKeyRecord) {
    Event event = fixedKeyRecord.value();

    // Forwarded after every handler ran: a failing handler sends none of the commands.
    List<Object> commandPayloads = new ArrayList<>();
    handlers.eventHandlers(event.getPayload().getClass()).stream()
        .sorted(Comparator.comparingInt(EventHandlerMethod::getPriority).reversed())
        .peek(handler -> log.debug("Handling event: eventType={}, aggregateId={}, sequence={}, handler={}.{}", event.getType(), event.getAggregateId(), event.getSequence(), handler.getMethod().getDeclaringClass().getSimpleName(), handler.getMethod().getName()))
        .forEach(handler -> commandPayloads.addAll(handler.handle(event)));

    if (!commandPayloads.isEmpty()) {
      context.forward(fixedKeyRecord.withValue(new HandledEvent(event, commandPayloads)));
    }
  }

  @Override
  public void close() {

  }
}
