package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.constants.Handlers;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class EventTransformer implements ValueTransformerWithKey<String, Event, Event> {

  private ProcessorContext context;

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Event transform(String key, Event event) {
    Collection<EventHandler> handlers = Handlers.EVENT_HANDLERS.get(event.getPayload().getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    handlers.stream()
        .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
        .forEach(handler ->
            handler.apply(event));

    return event;
  }

  @Override
  public void close() {

  }


}
