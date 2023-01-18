package io.github.alikelleci.eventify.support.serializer2;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.util.JacksonUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

  private Class<T> targetType;
  private final ObjectMapper objectMapper;

  public JsonSerializer() {
    this(null);
  }

  public JsonSerializer(Class<T> targetType) {
    this(targetType, JacksonUtils.enhancedObjectMapper());
  }

  public JsonSerializer(Class<T> targetType, ObjectMapper objectMapper) {
    this.targetType = targetType;
    this.objectMapper = objectMapper;
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, T object) {
    if (object == null) {
      return null;
    }

    try {
      return objectMapper.writeValueAsBytes(object);

    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON", ExceptionUtils.getRootCause(e));
    }
  }

  @Override
  public void close() {
  }

}
