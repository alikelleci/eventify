package io.github.alikelleci.eventify.core.event;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
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
  private final ObjectReader treeReader;

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
    // Decimals as stored, not as doubles: with upcasters an event must read the same as without.
    this.treeReader = objectMapper.reader()
        .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .without(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES);
  }

  /** A new serde with these upcasters added; this one is unchanged. */
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
        JsonNode upcasted = upcasters.upcast(treeReader.readTree(bytes));
        // Read from JSON again, not from the tree: the tree would turn a number in an Object field into a BigDecimal.
        return objectMapper.readValue(objectMapper.writeValueAsBytes(upcasted), Event.class);
      } catch (Exception e) {
        throw new SerializationException("Error deserializing event.", e);
      }
    };
  }
}
