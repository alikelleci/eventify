package io.github.alikelleci.eventify.messaging.eventhandling;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.eventify.EventifyConfig;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.Repository;
import io.github.alikelleci.eventify.messaging.upcasting.UpcasterUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class EventTransformer implements ValueTransformerWithKey<String, JsonNode, Event> {

  private final EventifyConfig eventifyConfig;

  private Repository repository;

  public EventTransformer(EventifyConfig eventifyConfig) {
    this.eventifyConfig = eventifyConfig;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.repository = new Repository(processorContext, eventifyConfig);
  }

  @Override
  public Event transform(String key, JsonNode jsonNode) {
    Event event = UpcasterUtil.upcast(eventifyConfig, jsonNode);

    Collection<EventHandler> handlers = eventifyConfig.getHandlers().eventHandlers().get(event.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(handlers)) {
      handlers.stream()
          .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
          .forEach(handler ->
              handler.apply(event));
    }

    EventSourcingHandler eventSourcingHandler = eventifyConfig.getHandlers().eventSourcingHandlers().get(event.getPayload().getClass());
    if (eventSourcingHandler != null) {
      repository.saveEvent(event);
    }

    return event;
  }

  @Override
  public void close() {

  }


}
