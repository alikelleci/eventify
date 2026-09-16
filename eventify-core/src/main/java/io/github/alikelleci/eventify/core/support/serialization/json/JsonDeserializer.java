package io.github.alikelleci.eventify.core.support.serialization.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.core.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.core.messaging.upcasting.exceptions.UpcastingException;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import io.github.alikelleci.eventify.core.util.HandlerUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    int storedRevision = jsonNode.path("revision").asInt(1);
    int revision = storedRevision;
    JsonNode payload = jsonNode.get("payload");

    // Each upcaster takes the payload as the one before it left it: whether it changed the node it was given, or
    // returned a new one. The tree is parsed for this read only, so changing it in place is safe.
    List<Upcaster> chain = upCasters.stream()
        .sorted(Comparator.comparingInt(JsonDeserializer::revisionOf))
        .toList();
    for (Upcaster upcaster : chain) {
      if (revisionOf(upcaster) != revision) {
        continue;
      }
      JsonNode upcasted = upcaster.apply(payload);
      if (upcasted == null) {
        break; // no upcasting from here: the payload stays at this revision
      }
      if (!(upcasted instanceof ObjectNode)) {
        throw new UpcastingException("Upcaster " + upcaster.getMethod() + " must return a JSON object, but returned: " + upcasted.getNodeType());
      }
      ((ObjectNode) upcasted).put("@class", className); // restore original typeInfo in case its changed
      payload = upcasted;
      revision++;
    }

    if (revision != storedRevision) {
      ((ObjectNode) jsonNode).set("payload", payload);
      ((ObjectNode) jsonNode).put("revision", revision);
    }
    return jsonNode;
  }

  private static int revisionOf(Upcaster upcaster) {
    return upcaster.getMethod().getAnnotation(Upcast.class).revision();
  }

  public JsonDeserializer<T> registerUpcaster(Object handler) {
    HandlerUtils.registerUpcasters(upcasters, handler);
    return this;
  }
}
