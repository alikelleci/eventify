package io.github.alikelleci.eventify.messaging.eventhandling;

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
public class Event extends Message {
  private String aggregateId;

  protected Event() {
    super(null, null, null, null);
  }

  protected Event(String id, Instant timestamp, Object payload, Metadata metadata, String aggregateId) {
    super(id, timestamp, payload, metadata);
    this.aggregateId = aggregateId;
  }

  public static EventBuilder builder() {
    return new EventBuilder();
  }

  public static class EventBuilder {
    private Instant timestamp;
    private Object payload;
    private Metadata metadata;

    private String aggregateId;

    public EventBuilder timestamp(Instant timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public EventBuilder payload(Object payload) {
      this.payload = payload;
      return this;
    }

    public EventBuilder metadata(Metadata metadata) {
      this.metadata = metadata;
      return this;
    }

    protected EventBuilder aggregateId(String aggregateId) {
      this.aggregateId = aggregateId;
      return this;
    }

    public Event build() {
      this.aggregateId = Optional
          .ofNullable(this.aggregateId)
          .orElse(createAggregateId(this.payload));

      String id = Optional.ofNullable(this.aggregateId)
          .map(s -> s + "@" + UlidCreator.getMonotonicUlid().toString())
          .orElse(null);

      return new Event(id, this.timestamp, this.payload, this.metadata, this.aggregateId);
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
