package io.github.alikelleci.eventify.messaging.upcasting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.upcasting.annotations.Upcast;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class EventUpcaster {

  private final Eventify eventify;

  public EventUpcaster(Eventify eventify) {
    this.eventify = eventify;
  }

  public JsonNode upcast(JsonNode jsonNode) {
    String className = Optional.ofNullable(jsonNode.get("payload"))
        .map(payload -> payload.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);

    if (StringUtils.isBlank(className)) {
      return jsonNode;
    }

    Collection<Upcaster> upcasters = eventify.getUpcasters().get(className);
    if (CollectionUtils.isEmpty(upcasters)) {
      return jsonNode;
    }

    AtomicInteger revision = new AtomicInteger(1);

    Optional.ofNullable(jsonNode.get("revision"))
        .map(JsonNode::intValue)
        .ifPresent(revision::set);

    JsonNode payload = eventify.getObjectMapper().convertValue(jsonNode.get("payload"), JsonNode.class);

    upcasters.stream()
        .sorted(Comparator.comparingInt(handler -> handler.getMethod().getAnnotation(Upcast.class).revision()))
        .filter(handler -> handler.getMethod().getAnnotation(Upcast.class).revision() == revision.get())
        .map(handler -> handler.apply(payload))
        .filter(Objects::nonNull)
        .forEach(upcastedPayload -> {
          ((ObjectNode) upcastedPayload).put("@class", className); // restore original typeInfo in case its changed
          ((ObjectNode) jsonNode).set("payload", upcastedPayload);
          ((ObjectNode) jsonNode).put("revision", revision.incrementAndGet());
        });

    return jsonNode;
  }

  public <T> T convert(JsonNode jsonNode, Class<T> targetType) {
    return eventify.getObjectMapper().convertValue(jsonNode, targetType);
  }

  public <T> T upcastAndConvert(JsonNode jsonNode, Class<T> targetType) {
    JsonNode upcasted = upcast(jsonNode);
    return convert(upcasted, targetType);
  }

}
