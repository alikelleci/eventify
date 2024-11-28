package io.github.alikelleci.eventify.messaging.eventhandling;

import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Event extends Message {
  String aggregateId;
  int revision;

  private Event() {
    this.aggregateId = null;
    this.revision = 1;
  }

  private Event(EventBuilder builder) {
    this.id = builder.id;
    this.timestamp = builder.timestamp;
    this.type = builder.type;
    this.payload = builder.payload;
    this.metadata = builder.metadata;
    this.aggregateId = builder.aggregateId;
    this.revision = builder.revision;
  }

  public static EventBuilder builder() {
    return new EventBuilder();
  }

  public EventBuilder toBuilder() {
    return new EventBuilder()
        .id(this.id)
        .type(this.type)
        .timestamp(this.timestamp)
        .payload(this.payload)
        .metadata(this.metadata)
        .aggregateId(this.aggregateId)
        .revision(this.revision);
  }

  public static class EventBuilder {
    private String id;
    private Instant timestamp;
    private String type;
    private Object payload;
    private Metadata metadata = Metadata.builder().build();
    private String aggregateId;
    private int revision = 1;

    private EventBuilder id(String id) {
      this.id = id;
      return this;
    }

    private EventBuilder type(String type) {
      this.type = type;
      return this;
    }

    public EventBuilder timestamp(Instant timestamp) {
      if (timestamp == null) {
        timestamp = Instant.now();
      }
      this.timestamp = timestamp;
      return this;
    }

    public EventBuilder payload(Object payload) {
      this.payload = payload;
      return this;
    }

    public EventBuilder metadata(String key, String value) {
      this.metadata = this.metadata.toBuilder()
          .entry(key, value)
          .build();
      return this;
    }

    public EventBuilder metadata(Metadata metadata) {
      Metadata.MetadataBuilder builder = this.metadata.toBuilder();
      if (metadata != null) {
        metadata.getEntries().forEach(builder::entry);
      }
      this.metadata = builder.build();
      return this;
    }

    private EventBuilder aggregateId(String aggregateId) {
      this.aggregateId = aggregateId;
      return this;
    }

    public EventBuilder revision(int revision) {
      this.revision = revision;
      return this;
    }

    public Event build() {
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
      return new Event(this);
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
