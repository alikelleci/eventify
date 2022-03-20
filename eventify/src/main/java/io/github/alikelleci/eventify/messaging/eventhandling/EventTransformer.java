package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.Repository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

@Slf4j
public class EventTransformer implements ValueTransformerWithKey<String, Event, Event> {

  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers;
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;

  private Repository repository;

  public EventTransformer(MultiValuedMap<Class<?>, EventHandler> eventHandlers, Map<Class<?>, EventSourcingHandler> eventSourcingHandlers) {
    eventHandlers = eventHandlers;
    eventSourcingHandlers = eventSourcingHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.repository = new Repository(processorContext, eventSourcingHandlers);
  }

  @Override
  public Event transform(String key, Event event) {
    Collection<EventHandler> handlers = eventHandlers.get(event.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(handlers)) {
      handlers.stream()
          .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
          .forEach(handler ->
              handler.apply(event));
    }

    EventSourcingHandler eventSourcingHandler = eventSourcingHandlers.get(event.getPayload().getClass());
    if (eventSourcingHandler != null) {
      repository.saveEvent(event);
    }

    return event;
  }

  @Override
  public void close() {

  }


}
