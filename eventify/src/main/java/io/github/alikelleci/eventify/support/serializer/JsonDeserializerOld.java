package io.github.alikelleci.eventify.support.serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.util.JacksonUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

@Deprecated
public class JsonDeserializerOld<T> implements Deserializer<T> {

  private final ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();
  private Class<T> type;

  public JsonDeserializerOld() {
  }

  public JsonDeserializerOld(Class<T> type) {
    this.type = type;
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
      JsonNode root = objectMapper.readTree(bytes);

      // Check if type info is available
      String className = Optional.ofNullable(root.get("@class"))
          .map(JsonNode::textValue)
          .orElse(null);

      // Added for backwards compatibility
      if (StringUtils.startsWith(className, "com.github.eventify.")) {
        // Extract payload
        root = root.get("payload");

        return objectMapper.convertValue(root, type);
      }

      return objectMapper.readValue(bytes, type);

    } catch (Exception e) {
      throw new SerializationException("Error deserializing JSON", ExceptionUtils.getRootCause(e));
    }
  }

  @Override
  public T deserialize(String topic, Headers headers, byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try {
      JsonNode root = objectMapper.readTree(bytes);

      // Check if type info is available
      String className = Optional.ofNullable(root.get("@class"))
          .map(JsonNode::textValue)
          .orElse(null);

      // Added for backwards compatibility
      if (StringUtils.startsWith(className, "com.github.eventify.")) {

        // Extract metadata
        JsonNode metadata = root.get("metadata");
        JsonNode entries = metadata.get("entries");

        IteratorUtils.toList(entries.fields()).stream()
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .filter(entry -> StringUtils.isNoneBlank(entry.getKey(), entry.getValue().textValue()))
            .forEach(entry -> {
              if (entry.getKey().equals("$result") && entry.getValue().textValue().equals("failed")) {
                headers.add(Metadata.RESULT, "failure".getBytes(StandardCharsets.UTF_8));
              } else if (entry.getKey().equals("$failure")) {
                headers.add(Metadata.CAUSE, entry.getValue().textValue().getBytes(StandardCharsets.UTF_8));
              } else {
                headers.add(entry.getKey(), entry.getValue().textValue().getBytes(StandardCharsets.UTF_8));
              }
            });

        // Extract payload
        root = root.get("payload");

        return objectMapper.convertValue(root, type);
      }

      return objectMapper.readValue(bytes, type);

    } catch (Exception e) {
      throw new SerializationException("Error deserializing JSON", ExceptionUtils.getRootCause(e));
    }

  }

  @Override
  public void close() {
  }

}
