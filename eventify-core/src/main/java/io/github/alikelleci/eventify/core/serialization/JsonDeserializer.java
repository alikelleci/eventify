package io.github.alikelleci.eventify.core.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.handler.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcast;
import io.github.alikelleci.eventify.core.upcasting.exception.UpcastingException;
import io.github.alikelleci.eventify.core.upcasting.internal.UpcasterMethod;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Optional;

public class JsonDeserializer<T> implements Deserializer<T> {

  private final Class<T> targetType;
  private final ObjectMapper objectMapper;
  private final MultiValuedMap<String, UpcasterMethod> upcasters;

  public JsonDeserializer(Class<T> targetType) {
    this(targetType, EventifyObjectMapper.get(), new ArrayListValuedHashMap<>());
  }

  public JsonDeserializer(Class<T> targetType, ObjectMapper objectMapper) {
    this(targetType, objectMapper, new ArrayListValuedHashMap<>());
  }

  public JsonDeserializer(Class<T> targetType, ObjectMapper objectMapper, MultiValuedMap<String, UpcasterMethod> upcasters) {
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
    String storedClassName = classNameOf(jsonNode.get("payload"));
    if (StringUtils.isBlank(storedClassName) || CollectionUtils.isEmpty(this.upcasters.get(storedClassName))) {
      return jsonNode;
    }

    int storedRevision = jsonNode.path("revision").asInt(1);
    int revision = storedRevision;
    String className = storedClassName;
    JsonNode payload = jsonNode.get("payload");

    // Each upcaster takes the payload as the one before it left it: whether it changed the node it was given, or
    // returned a new one. The tree is parsed for this read only, so changing it in place is safe.
    // An upcaster renames the event class by setting another "@class": the chain goes on with the upcasters of that
    // class, from the revision reached. Every step raises the revision, so the chain always ends.
    UpcasterMethod upcaster;
    while ((upcaster = upcasterOf(className, revision)) != null) {
      JsonNode upcasted = upcaster.apply(payload);
      if (upcasted == null) {
        break; // no upcasting from here: the payload stays at this revision
      }
      if (!(upcasted instanceof ObjectNode upcastedObject)) {
        throw new UpcastingException("UpcasterMethod " + upcaster.getMethod() + " must return a JSON object, but returned: " + upcasted.getNodeType());
      }
      String upcastedClassName = classNameOf(upcastedObject);
      if (StringUtils.isBlank(upcastedClassName)) {
        upcastedObject.put("@class", className); // a new node built without it: still the same class
      } else {
        className = upcastedClassName;
      }
      payload = upcastedObject;
      revision++;
    }

    if (revision != storedRevision) {
      ((ObjectNode) jsonNode).set("payload", payload);
      ((ObjectNode) jsonNode).put("revision", revision);
      if (!className.equals(storedClassName) && jsonNode.has("type")) {
        ((ObjectNode) jsonNode).put("type", simpleNameOf(className));
      }
    }
    return jsonNode;
  }

  private UpcasterMethod upcasterOf(String className, int revision) {
    return this.upcasters.get(className).stream()
        .filter(upcaster -> revisionOf(upcaster) == revision)
        .findFirst()
        .orElse(null);
  }

  private static String classNameOf(JsonNode payload) {
    return Optional.ofNullable(payload)
        .map(node -> node.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);
  }

  /** As {@link Class#getSimpleName()} gives it, without loading the class: "com.example.OrderEvent$OrderPlaced" is "OrderPlaced". */
  private static String simpleNameOf(String className) {
    String name = className.substring(className.lastIndexOf('.') + 1);
    return name.substring(name.lastIndexOf('$') + 1);
  }

  private static int revisionOf(UpcasterMethod upcaster) {
    return upcaster.getMethod().getAnnotation(Upcast.class).revision();
  }

  public JsonDeserializer<T> registerUpcaster(Object handler) {
    HandlerRegistry.registerUpcasters(upcasters, handler);
    return this;
  }
}
