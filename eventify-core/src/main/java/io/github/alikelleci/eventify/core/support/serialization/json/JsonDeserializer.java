package io.github.alikelleci.eventify.core.support.serialization.json;

import io.github.alikelleci.eventify.core.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.core.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import io.github.alikelleci.eventify.core.util.AnnotationUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonDeserializer<T> implements Deserializer<T> {

  private final Class<T> targetType;
  private final ObjectMapper objectMapper;
  private final MultiValuedMap<String, Upcaster> upcasters;

  public JsonDeserializer() {
    this(null);
  }

  public JsonDeserializer(Class<T> targetType) {
    this(targetType, JacksonUtils.enhancedObjectMapper(), new ArrayListValuedHashMap<>());

  }

  public JsonDeserializer(Class<T> targetType, ObjectMapper objectMapper) {
    this(targetType, objectMapper, new ArrayListValuedHashMap<>());
  }

  public JsonDeserializer(Class<T> targetType, ObjectMapper objectMapper, MultiValuedMap<String, Upcaster> upcasters) {
    this.targetType = targetType;
    this.objectMapper = objectMapper;
    this.upcasters = upcasters;
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    if (bytes == null) return null;
    try {
      if (upcasters == null || upcasters.isEmpty()) {
        return objectMapper.readValue(bytes, targetType);
      }
      JsonNode jsonNode = objectMapper.readTree(bytes);
      JsonNode upcasted = upcast(jsonNode);
      return objectMapper.convertValue(upcasted, targetType);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing JSON", e);
    }
  }

  @Override
  public void close() {
  }


  private JsonNode upcast(JsonNode jsonNode) {
    String className = Optional.ofNullable(jsonNode.get("payload"))
        .map(payload -> payload.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);

    if (StringUtils.isBlank(className)) {
      return jsonNode;
    }

    Collection<Upcaster> upCasters = this.upcasters.get(className);
    if (CollectionUtils.isEmpty(upCasters)) {
      return jsonNode;
    }

    AtomicInteger revision = new AtomicInteger(1);

    Optional.ofNullable(jsonNode.get("revision"))
        .map(JsonNode::intValue)
        .ifPresent(revision::set);

    JsonNode payload = this.objectMapper.convertValue(jsonNode.get("payload"), JsonNode.class);

    upCasters.stream()
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

  public JsonDeserializer<T> registerUpcaster(Object handler) {
    AnnotationUtils.findAnnotatedMethods(handler.getClass(), Upcast.class)
        .forEach(method -> addUpcaster(handler, method));

    return this;
  }

  private void addUpcaster(Object listener, Method method) {
    if (method.getParameterCount() == 1) {
      String type = method.getAnnotation(Upcast.class).type();
      upcasters.put(type, new Upcaster(listener, method));
    }
  }
}
