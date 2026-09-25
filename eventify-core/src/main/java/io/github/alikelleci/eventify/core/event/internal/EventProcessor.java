package io.github.alikelleci.eventify.core.event.internal;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.internal.HandlerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.util.Comparator;

@Slf4j
public class EventProcessor implements FixedKeyProcessor<String, Event, Event> {

  private final HandlerRegistry handlers;
  private FixedKeyProcessorContext<String, Event> context;

  public EventProcessor(HandlerRegistry handlers) {
    this.handlers = handlers;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, Event> context) {
    this.context = context;
  }

  @Override
  public void process(FixedKeyRecord<String, Event> fixedKeyRecord) {
    Event event = fixedKeyRecord.value();

    handlers.eventHandlers(event.getPayload().getClass()).stream()
        .sorted(Comparator.comparingInt(EventHandlerMethod::getPriority).reversed())
        .peek(handler -> log.debug("Handling event: {} ({})", event.getType(), event.getAggregateId()))
        .forEach(handler -> handler.handle(event));

    context.forward(fixedKeyRecord);
  }

  @Override
  public void close() {

  }
}
