package io.github.alikelleci.eventify.core.messaging.eventhandling;

import io.github.alikelleci.eventify.core.Eventify;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class EventProcessor implements FixedKeyProcessor<String, Event, Event> {

  private final Eventify eventify;
  private FixedKeyProcessorContext<String, Event> context;

  public EventProcessor(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, Event> context) {
    this.context = context;
  }

  @Override
  public void process(FixedKeyRecord<String, Event> fixedKeyRecord) {
    Event event = fixedKeyRecord.value();

    Collection<EventHandler> eventHandlers = eventify.getEventHandlers().get(event.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(eventHandlers)) {
      eventHandlers.stream()
          .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
          .peek(handler -> log.debug("Handling event: {} ({})", event.getType(), event.getAggregateId()))
          .forEach(handler -> handler.apply(event));
    }

    context.forward(fixedKeyRecord);
  }

  @Override
  public void close() {
  }
}
