package io.github.alikelleci.eventify.core.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.upcasting.Upcasters;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

  private final Class<T> targetType;
  private final ObjectMapper objectMapper;
  private final Upcasters upcasters;

  public JsonDeserializer(Class<T> targetType) {
    this(targetType, EventifyObjectMapper.create(), new Upcasters());
  }

  public JsonDeserializer(Class<T> targetType, ObjectMapper objectMapper) {
    this(targetType, objectMapper, new Upcasters());
  }

  public JsonDeserializer(Class<T> targetType, ObjectMapper objectMapper, Upcasters upcasters) {
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
      JsonNode upcasted = upcasters.upcast(jsonNode);
      return objectMapper.convertValue(upcasted, targetType);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing JSON", e);
    }
  }

  @Override
  public void close() {
  }

  /** Adds the {@code @Upcast} methods of the object to the upcasters this deserializer reads with. */
  public JsonDeserializer<T> registerUpcaster(Object handler) {
    upcasters.register(handler);
    return this;
  }
}
