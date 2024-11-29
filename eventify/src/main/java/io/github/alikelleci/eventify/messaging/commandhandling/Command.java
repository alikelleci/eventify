package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.util.IdUtils;
import lombok.Value;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;

@Value
public class Command implements Message {
  String id;
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  String aggregateId;

  private Command() {
    this.id = null;
    this.timestamp = null;
    this.type = null;
    this.payload = null;
    this.metadata = null;
    this.aggregateId = null;
  }

  private Command(CommandBuilder builder) {
    this.id = builder.id;
    this.timestamp = builder.timestamp;
    this.type = builder.type;
    this.payload = builder.payload;
    this.metadata = builder.metadata;
    this.aggregateId = builder.aggregateId;
  }

  public CommandBuilder toBuilder() {
    return new CommandBuilder()
        .id(this.id)
        .type(this.type)
        .timestamp(this.timestamp)
        .payload(this.payload)
        .addMetadata(this.metadata)
        .aggregateId(this.aggregateId);
  }

  public static CommandBuilder builder() {
    return new CommandBuilder();
  }

  public static class CommandBuilder {
    private String id;
    private Instant timestamp;
    private String type;
    private Object payload;
    private Metadata metadata = Metadata.builder().build();
    private String aggregateId;

    private CommandBuilder id(String id) {
      this.id = id;
      return this;
    }

    private CommandBuilder type(String type) {
      this.type = type;
      return this;
    }

    public CommandBuilder timestamp(Instant timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public CommandBuilder payload(Object payload) {
      this.payload = payload;
      return this;
    }

    public CommandBuilder addMetadata(String key, String value) {
      this.metadata = this.metadata.toBuilder()
          .entry(key, value)
          .build();
      return this;
    }

    public CommandBuilder addMetadata(Metadata metadata) {
      Metadata.MetadataBuilder builder = this.metadata.toBuilder();
      if (metadata != null) {
        metadata.getEntries().forEach(builder::entry);
      }
      this.metadata = builder.build();
      return this;
    }

    public CommandBuilder removeMetadata(String key) {
      Map<String, String> map = new HashMap<>(this.metadata.getEntries());
      map.keySet().removeIf(k -> k.equals(key));
      this.metadata = this.metadata.toBuilder()
          .clearEntries()
          .entries(map)
          .build();
      return this;
    }

    private CommandBuilder aggregateId(String aggregateId) {
      this.aggregateId = aggregateId;
      return this;
    }

    public Command build() {
      if (this.payload == null) {
        throw new PayloadMissingException("Message payload is missing.");
      }
      this.type = this.payload.getClass().getSimpleName();
      this.aggregateId = IdUtils.getAggregateId(this.payload);

      if (this.timestamp == null) {
        this.timestamp = Instant.now();
      }
      if (this.id == null) {
        this.id = IdUtils.createCompoundKey(this.aggregateId, this.timestamp);
      }
      if (metadata.getCorrelationId() == null) {
        this.metadata = metadata.toBuilder().entry(CORRELATION_ID, UUID.randomUUID().toString()).build();
      }

      return new Command(this);
    }
  }
}
