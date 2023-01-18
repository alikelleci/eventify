package io.github.alikelleci.eventify.support.serializer2;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.util.JacksonUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

  private Class<T> type;
  private final ObjectMapper objectMapper;


  public JsonDeserializer() {
    this(null, JacksonUtils.enhancedObjectMapper());
  }

  public JsonDeserializer(Class<T> type, ObjectMapper objectMapper) {
    this.type = type;
    this.objectMapper = objectMapper;
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
      return objectMapper.readValue(bytes, type);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing JSON", ExceptionUtils.getRootCause(e));
    }
  }

  @Override
  public void close() {
  }

}
