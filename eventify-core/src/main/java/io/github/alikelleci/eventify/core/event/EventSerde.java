package io.github.alikelleci.eventify.core.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import io.github.alikelleci.eventify.core.upcasting.Upcasters;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Reads and writes events as JSON. An event is read upcast to the latest revision of its payload, with the upcasters
 * registered here: e.g. in a Kafka Streams service that reads an events topic,
 * {@code new EventSerde().registerUpcaster(new OrderEventUpcaster())}.
 */
public class EventSerde implements Serde<Event> {

  private final ObjectMapper objectMapper;
  private final Upcasters upcasters;
  private final JsonSerializer<Event> serializer;

  public EventSerde() {
    this(EventifyObjectMapper.create());
  }

  public EventSerde(ObjectMapper objectMapper) {
    this(objectMapper, new Upcasters());
  }

  /** With these upcasters, the upcasters themselves: ones registered with them later are used too. */
  public EventSerde(ObjectMapper objectMapper, Upcasters upcasters) {
    this.objectMapper = objectMapper;
    this.upcasters = upcasters;
    this.serializer = new JsonSerializer<>(objectMapper);
  }

  /** Adds the {@code @Upcast} methods of the object to the upcasters events are read with. */
  public EventSerde registerUpcaster(Object handler) {
    upcasters.register(handler);
    return this;
  }

  @Override
  public Serializer<Event> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<Event> deserializer() {
    return (topic, bytes) -> {
      if (bytes == null) {
        return null;
      }
      try {
        if (upcasters.isEmpty()) {
          return objectMapper.readValue(bytes, Event.class);
        }
        JsonNode upcasted = upcasters.upcast(objectMapper.readTree(bytes));
        return objectMapper.convertValue(upcasted, Event.class);
      } catch (Exception e) {
        throw new SerializationException("Error deserializing event", e);
      }
    };
  }
}
