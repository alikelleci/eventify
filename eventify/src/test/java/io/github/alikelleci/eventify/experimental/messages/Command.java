package io.github.alikelleci.eventify.experimental.messages;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;
import java.util.Optional;

@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Command extends Message {
  private String aggregateId;

  protected Command() {
    super(null, null, null, null);
  }

  protected Command(String id, Instant timestamp, Object payload, Metadata metadata) {
    super(id, timestamp, payload, metadata);
    this.aggregateId = createAggregatieId(payload);
  }

  public Command(Object payload, Metadata metadata, Instant timestamp) {
    this(null, timestamp, payload, metadata);
  }

  public Command(Object payload, Metadata metadata) {
    this(null, null, payload, metadata);
  }

  public Command(Object payload) {
    this(null, null, payload, null);
  }

  private String createAggregatieId(Object payload) {
    return Optional.ofNullable(payload).flatMap(p -> FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class).stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> {
          field.setAccessible(true);
          return (String) ReflectionUtils.getField(field, payload);
        }))
        .orElse(null);
  }

  public static CommandBuilder builder() {
    return new CommandBuilder();
  }

  public static class CommandBuilder {
    private Object payload;
    private Metadata metadata;
    private Instant timestamp;

    public CommandBuilder payload(Object payload) {
      this.payload = payload;
      return this;
    }

    public CommandBuilder metadata(Metadata metadata) {
      this.metadata = metadata;
      return this;
    }

    public CommandBuilder timestamp(Instant timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Command build() {
      return new Command(this.payload, this.metadata, this.timestamp);
    }
  }
}
