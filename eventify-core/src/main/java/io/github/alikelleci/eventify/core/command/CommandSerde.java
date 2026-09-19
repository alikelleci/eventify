package io.github.alikelleci.eventify.core.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** Reads and writes commands as JSON. Commands are not upcast: they are handled once, not kept. */
public class CommandSerde implements Serde<Command> {

  private final JsonSerializer<Command> serializer;
  private final JsonDeserializer<Command> deserializer;

  public CommandSerde() {
    this(EventifyObjectMapper.create());
  }

  public CommandSerde(ObjectMapper objectMapper) {
    this.serializer = new JsonSerializer<>(objectMapper);
    this.deserializer = new JsonDeserializer<>(Command.class, objectMapper);
  }

  @Override
  public Serializer<Command> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<Command> deserializer() {
    return deserializer;
  }
}
