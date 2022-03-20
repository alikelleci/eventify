package io.github.alikelleci.eventify.messaging.commandhandling;

import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
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

  protected Command(String id, Instant timestamp, Object payload, Metadata metadata, String aggregateId) {
    super(id, timestamp, payload, metadata);
    this.aggregateId = aggregateId;
  }

  public static CommandBuilder builder() {
    return new CommandBuilder();
  }

  public static class CommandBuilder {
    private Instant timestamp;
    private Object payload;
    private Metadata metadata;

    private String aggregateId;

    public CommandBuilder timestamp(Instant timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public CommandBuilder payload(Object payload) {
      this.payload = payload;
      return this;
    }

    public CommandBuilder metadata(Metadata metadata) {
      this.metadata = metadata;
      return this;
    }

    protected CommandBuilder aggregateId(String aggregateId) {
      this.aggregateId = aggregateId;
      return this;
    }

    public Command build() {
      this.aggregateId = Optional
          .ofNullable(this.aggregateId)
          .orElse(createAggregateId(this.payload));

      String id = Optional.ofNullable(this.aggregateId)
          .map(s -> s + "@" + UlidCreator.getMonotonicUlid().toString())
          .orElse(null);

      return new Command(id, this.timestamp, this.payload, this.metadata, this.aggregateId);
    }

    private String createAggregateId(Object payload) {
      return Optional.ofNullable(payload).flatMap(p -> FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class).stream()
          .filter(field -> field.getType() == String.class)
          .findFirst()
          .map(field -> {
            field.setAccessible(true);
            return (String) ReflectionUtils.getField(field, payload);
          }))
          .orElse(null);
    }
  }
}
