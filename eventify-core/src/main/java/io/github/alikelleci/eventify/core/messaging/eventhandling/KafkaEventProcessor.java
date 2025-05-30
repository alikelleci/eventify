package io.github.alikelleci.eventify.core.messaging.eventhandling;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import java.util.Collection;
import java.util.Comparator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class KafkaEventProcessor implements EventProcessor, FixedKeyProcessor<String, Event, Event> {

  private final Eventify eventify;
  private FixedKeyProcessorContext<String, Event> context;
  private KeyValueStore<String, Event> eventStore;

  public KafkaEventProcessor(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, Event> context) {
    this.context = context;
    this.eventStore = context.getStateStore("event-store");
  }

  @Override
  public void process(FixedKeyRecord<String, Event> fixedKeyRecord) {
    Event event = fixedKeyRecord.value();

    Collection<EventHandler> eventHandlers = eventify.getEventHandlers().get(event.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(eventHandlers)) {
      eventHandlers.stream()
          .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
          .peek(handler -> log.debug("Handling event: {} ({})", event.getType(), event.getAggregateId()))
          .forEach(handler ->
              handler.apply(event));
    }

    EventSourcingHandler eventSourcingHandler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
    if (eventSourcingHandler != null) {
      saveEvent(event);
    }

    context.forward(fixedKeyRecord);
  }

  @Override
  public void close() {

  }

  private void saveEvent(Event event) {
    eventStore.putIfAbsent(event.getId(), event);
  }

}
