package io.github.alikelleci.eventify.messaging.eventhandling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.Config;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.Repository;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.alikelleci.eventify.messaging.Metadata.REVISION;

@Slf4j
public class EventTransformer implements ValueTransformerWithKey<String, JsonNode, Event> {

  private final Config config;

  private Repository repository;

  public EventTransformer(Config config) {
    this.config = config;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.repository = new Repository(processorContext, config);
  }

  @Override
  public Event transform(String key, JsonNode jsonNode) {
    String className = Optional.ofNullable(jsonNode)
        .map(node -> node.get("payload"))
        .map(node -> node.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);

    if (StringUtils.isBlank(className)) {
      return null;
    }


    Event event;

    Collection<Upcaster> upcasters = config.handlers().upcasters().get(className);
    if (CollectionUtils.isEmpty(upcasters)) {
      event = JacksonUtils.enhancedObjectMapper().convertValue(jsonNode, Event.class);
    }

    AtomicInteger revision = Optional.ofNullable(jsonNode.get("metadata"))
        .map(m -> m.get(REVISION))
        .map(JsonNode::intValue)
        .map(AtomicInteger::new)
        .orElse(new AtomicInteger(1));

    upcasters.stream()
        .sorted(Comparator.comparingInt(handler -> handler.getMethod().getAnnotation(Upcast.class).revision()))
        .filter(handler -> handler.getMethod().getAnnotation(Upcast.class).revision() == revision.get())
        .map(handler -> handler.apply(jsonNode.get("payload")))
        .forEach(result -> {
          ((ObjectNode) jsonNode.get("metadata")).put(REVISION, revision.incrementAndGet());
        });

    System.out.println(jsonNode);
    event = JacksonUtils.enhancedObjectMapper().convertValue(jsonNode, Event.class);

//    Collection<EventHandler> handlers = config.handlers().eventHandlers().get(event.getPayload().getClass());
//    if (CollectionUtils.isNotEmpty(handlers)) {
//      handlers.stream()
//          .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
//          .forEach(handler ->
//              handler.apply(event));
//    }
//
//    EventSourcingHandler eventSourcingHandler = config.handlers().eventSourcingHandlers().get(event.getPayload().getClass());
//    if (eventSourcingHandler != null) {
//      repository.saveEvent(event);
//    }

    return event;
  }

  @Override
  public void close() {

  }


}
