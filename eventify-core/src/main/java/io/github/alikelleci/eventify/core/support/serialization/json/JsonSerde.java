package io.github.alikelleci.eventify.core.support.serialization.json;

import io.github.alikelleci.eventify.core.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

public class JsonSerde<T> implements Serde<T> {
  private final JsonSerializer<T> serializer;
  private final JsonDeserializer<T> deserializer;

  public JsonSerde(Class<T> targetType) {
    this(targetType, JacksonUtils.enhancedObjectMapper(), new ArrayListValuedHashMap<>());
  }

  public JsonSerde(Class<T> targetType, ObjectMapper objectMapper) {
    this(targetType, objectMapper, new ArrayListValuedHashMap<>());
  }

  public JsonSerde(Class<T> targetType, ObjectMapper objectMapper, MultiValuedMap<String, Upcaster> upcasters) {
    this.serializer = new JsonSerializer<>(objectMapper);
    this.deserializer = new JsonDeserializer<>(targetType, objectMapper, upcasters);
  }

  @Override
  public Serializer<T> serializer() {
    return this.serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return this.deserializer;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.serializer.configure(configs, isKey);
    this.deserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    this.serializer.close();
    this.deserializer.close();
  }

  public JsonSerde<T> registerUpcaster(Object listener) {
    this.deserializer.registerUpcaster(listener);
    return this;
  }
}
