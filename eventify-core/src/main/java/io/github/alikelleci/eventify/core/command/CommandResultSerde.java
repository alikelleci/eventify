package io.github.alikelleci.eventify.core.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** Reads and writes the outcome of a command, including its nested command and produced events. */
public final class CommandResultSerde implements Serde<CommandResult> {

  private final JsonSerializer<CommandResult> serializer;
  private final JsonDeserializer<CommandResult> deserializer;

  public CommandResultSerde(ObjectMapper objectMapper) {
    this.serializer = new JsonSerializer<>(objectMapper);
    this.deserializer = new JsonDeserializer<>(CommandResult.class, objectMapper);
  }

  @Override
  public Serializer<CommandResult> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<CommandResult> deserializer() {
    return deserializer;
  }
}
