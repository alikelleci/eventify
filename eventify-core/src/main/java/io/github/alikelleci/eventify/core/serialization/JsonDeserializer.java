package io.github.alikelleci.eventify.core.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/** Reads JSON as the given type. Events are read with {@link io.github.alikelleci.eventify.core.event.EventSerde}: it upcasts them. */
public class JsonDeserializer<T> implements Deserializer<T> {

  private final Class<T> targetType;
  private final ObjectMapper objectMapper;

  public JsonDeserializer(Class<T> targetType) {
    this(targetType, EventifyObjectMapper.create());
  }

  public JsonDeserializer(Class<T> targetType, ObjectMapper objectMapper) {
    this.targetType = targetType;
    this.objectMapper = objectMapper;
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    if (bytes == null) return null;
    try {
      return objectMapper.readValue(bytes, targetType);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing JSON", e);
    }
  }

  @Override
  public void close() {
  }
}
