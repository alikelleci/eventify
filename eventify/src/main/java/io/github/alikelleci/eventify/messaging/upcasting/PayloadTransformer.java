package io.github.alikelleci.eventify.messaging.upcasting;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.eventify.EventifyConfig;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.upcasting.annotations.Upcast;
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


public class PayloadTransformer implements ValueTransformerWithKey<String, JsonNode, JsonNode> {

  private final EventifyConfig config;

  public PayloadTransformer(EventifyConfig config) {
    this.config = config;
  }

  @Override
  public void init(ProcessorContext processorContext) {
  }

  @Override
  public JsonNode transform(String key, JsonNode jsonNode) {
    String className = Optional.ofNullable(jsonNode)
        .map(node -> node.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);

    if (StringUtils.isBlank(className)) {
      return null;
    }

    Collection<Upcaster> handlers = config.getHandlers().upcasters().get(className);
    if (CollectionUtils.isEmpty(handlers)) {
      return jsonNode;
    }

    Metadata metadata = new Metadata();

    AtomicInteger revision = new AtomicInteger(Optional.ofNullable(metadata.get(Metadata.REVISION))
        .map(s -> NumberUtils.toInt(s, 1))
        .orElse(1));

    handlers.stream()
        .sorted(Comparator.comparingInt(handler -> handler.getMethod().getAnnotation(Upcast.class).revision()))
        .filter(handler -> handler.getMethod().getAnnotation(Upcast.class).revision() == revision.get())
        .map(handler -> handler.apply(jsonNode))
        .filter(Objects::nonNull)
        .forEach(result -> revision.incrementAndGet());

    return jsonNode;
  }

  @Override
  public void close() {

  }


}
