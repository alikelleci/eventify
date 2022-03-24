package io.github.alikelleci.eventify.messaging.upcasting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.util.JacksonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.alikelleci.eventify.messaging.Metadata.REVISION;


public class PayloadTransformer implements ValueTransformerWithKey<String, JsonNode, Event> {

  private final Eventify eventify;

  public PayloadTransformer(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public void init(ProcessorContext processorContext) {
  }

  @Override
  public Event transform(String key, JsonNode jsonNode) {
    JsonNode payload = jsonNode.get("payload");
    if (payload == null) {
      return null;
    }

    String className = Optional.ofNullable(payload.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);

    if (StringUtils.isBlank(className)) {
      return null;
    }

    Collection<Upcaster> upcasters = eventify.getUpcasters().get(className);
    if (CollectionUtils.isEmpty(upcasters)) {
      return JacksonUtils.enhancedObjectMapper().convertValue(jsonNode, Event.class);
    }

    ObjectNode metadata = (ObjectNode) jsonNode.get("metadata");
    if (metadata == null) {
      return null;
    }

    AtomicInteger revision = new AtomicInteger(1);

    Optional.ofNullable(metadata.get(REVISION))
        .map(JsonNode::intValue)
        .ifPresent(revision::set);

    upcasters.stream()
        .sorted(Comparator.comparingInt(handler -> handler.getMethod().getAnnotation(Upcast.class).revision()))
        .filter(handler -> handler.getMethod().getAnnotation(Upcast.class).revision() == revision.get())
        .map(handler -> handler.apply(payload))
        .forEach(result -> metadata.put(REVISION, revision.incrementAndGet()));

    return JacksonUtils.enhancedObjectMapper().convertValue(jsonNode, Event.class);
  }

  @Override
  public void close() {

  }


}
