package io.github.alikelleci.eventify.support.serialization.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.support.serialization.json.util.JacksonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
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
    if (bytes == null) {
      return null;
    }

    try {
      if (upcasters == null || upcasters.isEmpty()) {
        return objectMapper.readValue(bytes, targetType);
      }
      JsonNode jsonNode = objectMapper.readTree(bytes);
      JsonNode upcasted = upcast(jsonNode);
      return objectMapper.convertValue(upcasted, targetType);
    } catch (Exception e) {
      throw new SerializationException("Failed to deserialize JSON data for topic: " + topic, ExceptionUtils.getRootCause(e));
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
    List<Method> upcasterMethods = findMethodsWithAnnotation(handler.getClass(), Upcast.class);

    upcasterMethods
        .forEach(method -> addUpcaster(handler, method));

    return this;
  }

  private <A extends Annotation> List<Method> findMethodsWithAnnotation(Class<?> c, Class<A> annotation) {
    List<Method> methods = new ArrayList<>();
    for (Method method : c.getDeclaredMethods()) {
      if (AnnotationUtils.findAnnotation(method, annotation) != null) {
        methods.add(method);
      }
    }
    return methods;
  }

  private void addUpcaster(Object listener, Method method) {
    if (method.getParameterCount() == 1) {
      String type = method.getAnnotation(Upcast.class).type();
      upcasters.put(type, new Upcaster(listener, method));
    }
  }
}