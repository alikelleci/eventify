package io.github.alikelleci.eventify.core.aggregate.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** A snapshot whose aggregate can't be read is read without payload, so the aggregate is rebuilt from its events. */
public class SnapshotSerde implements Serde<AggregateState> {

  private final ObjectMapper objectMapper;
  private final JsonSerializer<AggregateState> serializer;
  private final JsonDeserializer<AggregateState> deserializer;

  public SnapshotSerde(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
    this.serializer = new JsonSerializer<>(objectMapper);
    this.deserializer = new JsonDeserializer<>(AggregateState.class, objectMapper);
  }

  @Override
  public Serializer<AggregateState> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<AggregateState> deserializer() {
    return (topic, bytes) -> {
      try {
        return deserializer.deserialize(topic, bytes);
      } catch (SerializationException e) {
        return withoutAggregate(bytes, e);
      }
    };
  }

  private AggregateState withoutAggregate(byte[] bytes, SerializationException cause) {
    try {
      ObjectNode json = (ObjectNode) objectMapper.readTree(bytes);
      json.remove("payload");
      return objectMapper.treeToValue(json, AggregateState.class);
    } catch (Exception e) {
      cause.addSuppressed(e);
      throw cause; // not even the snapshot itself can be read
    }
  }
}
