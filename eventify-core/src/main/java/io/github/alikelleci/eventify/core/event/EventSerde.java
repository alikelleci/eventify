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

/** Events as JSON, upcast on read, e.g. {@code new EventSerde().withUpcasters(new OrderEventUpcaster())}. */
public class EventSerde implements Serde<Event> {

  private final ObjectMapper objectMapper;
  private final Upcasters upcasters;
  private final JsonSerializer<Event> serializer;

  public EventSerde() {
    this(EventifyObjectMapper.create());
  }

  public EventSerde(ObjectMapper objectMapper) {
    this(objectMapper, Upcasters.NONE);
  }

  public EventSerde(ObjectMapper objectMapper, Upcasters upcasters) {
    this.objectMapper = objectMapper;
    this.upcasters = upcasters;
    this.serializer = new JsonSerializer<>(objectMapper);
  }

  /** A new serde that also upcasts with the {@code @Upcast} methods of these objects; this one stays as it is. */
  public EventSerde withUpcasters(Object... handlers) {
    return new EventSerde(objectMapper, upcasters.with(handlers));
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
