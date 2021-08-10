package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.constants.Handlers;
import io.github.alikelleci.eventify.messaging.Repository;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class EventSourcingTransformer implements ValueTransformerWithKey<String, Event, Event> {

  private Repository repository;

  @Override
  public void init(ProcessorContext processorContext) {
    this.repository = new Repository(processorContext);
  }

  @Override
  public Event transform(String key, Event event) {
    EventSourcingHandler eventSourcingHandler = Handlers.EVENTSOURCING_HANDLERS.get(event.getPayload().getClass());
    if (eventSourcingHandler == null) {
      return null;
    }

    repository.saveEvent(event);

    return event;
  }

  @Override
  public void close() {

  }

}
