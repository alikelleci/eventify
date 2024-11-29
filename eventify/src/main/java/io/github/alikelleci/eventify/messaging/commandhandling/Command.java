package io.github.alikelleci.eventify.messaging.commandhandling;

import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import lombok.Value;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;

@Value
public class Command implements Message {
  private String id;
  private Instant timestamp;
  private String type;
  private Object payload;
  private Metadata metadata;
  private String aggregateId;

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

  public static CommandBuilder builder() {
    return new CommandBuilder();
  }

  public CommandBuilder toBuilder() {
    return new CommandBuilder()
        .id(this.id)
        .type(this.type)
        .timestamp(this.timestamp)
        .payload(this.payload)
        .metadata(this.metadata)
        .aggregateId(this.aggregateId);
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

    public CommandBuilder metadata(String key, String value) {
      this.metadata = this.metadata.toBuilder()
          .entry(key, value)
          .build();
      return this;
    }

    public CommandBuilder metadata(Metadata metadata) {
      Metadata.MetadataBuilder builder = this.metadata.toBuilder();
      if (metadata != null) {
        metadata.getEntries().forEach(builder::entry);
      }
      this.metadata = builder.build();
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
      if (this.timestamp == null) {
        this.timestamp = Instant.now();
      }
      if (this.type == null) {
        this.type = payload.getClass().getSimpleName();
      }
      if (this.aggregateId == null) {
        this.aggregateId = getAggregateId(this.payload);
      }
      if (this.id == null) {
        this.id = this.aggregateId + "@" + UlidCreator.getMonotonicUlid(this.timestamp.toEpochMilli()).toString();
      }

      return new Command(this);
    }

    private String getAggregateId(Object payload) {
      return FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class)
          .stream()
          .filter(field -> field.getType() == String.class)
          .findFirst()
          .map(field -> {
            field.setAccessible(true);
            return (String) ReflectionUtils.getField(field, this.payload);
          })
          .orElseThrow(() -> new AggregateIdMissingException("Aggregate identifier missing. Please annotate your field containing the identifier with @AggregateId."));
    }
  }
}
