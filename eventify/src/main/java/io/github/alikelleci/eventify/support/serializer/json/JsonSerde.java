package io.github.alikelleci.eventify.support.serializer.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.support.serializer.json.util.JacksonUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerde<T> implements Serde<T> {
  private final JsonSerializer<T> jsonSerializer;
  private final JsonDeserializer<T> jsonDeserializer;

  public JsonSerde(Class<T> targetType) {
    this(targetType, JacksonUtils.enhancedObjectMapper(), new ArrayListValuedHashMap<>());
  }

  public JsonSerde(Class<T> targetType, ObjectMapper objectMapper) {
    this(targetType, objectMapper, new ArrayListValuedHashMap<>());
  }

  public JsonSerde(Class<T> targetType, ObjectMapper objectMapper, MultiValuedMap<String, Upcaster> upcasters) {
    this.jsonSerializer = new JsonSerializer<>(objectMapper);
    this.jsonDeserializer = new JsonDeserializer<>(targetType, objectMapper, upcasters);
  }

  @Override
  public Serializer<T> serializer() {
    return this.jsonSerializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return this.jsonDeserializer;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.jsonSerializer.configure(configs, isKey);
    this.jsonDeserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    this.jsonSerializer.close();
    this.jsonDeserializer.close();
  }

  public JsonSerde<T> registerUpcaster(Object listener) {
    this.jsonDeserializer.registerUpcaster(listener);
    return this;
  }
}
