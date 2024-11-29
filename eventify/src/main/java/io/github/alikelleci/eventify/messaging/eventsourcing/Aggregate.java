package io.github.alikelleci.eventify.messaging.eventsourcing;

import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.EnableSnapshotting;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import lombok.Value;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Value
public class Aggregate implements Message {
  private String id;
  private Instant timestamp;
  private String type;
  private Object payload;
  private Metadata metadata;
  private String aggregateId;
  private String eventId;
  private long version;

  private Aggregate() {
    this.id = null;
    this.timestamp = null;
    this.type = null;
    this.payload = null;
    this.metadata = null;
    this.aggregateId = null;
    this.eventId = null;
    this.version = 0;
  }

  private Aggregate(AggregateBuilder builder) {
    this.id = builder.id;
    this.timestamp = builder.timestamp;
    this.type = builder.type;
    this.payload = builder.payload;
    this.metadata = builder.metadata;
    this.aggregateId = builder.aggregateId;
    this.eventId = builder.eventId;
    this.version = builder.version;
  }

  public static AggregateBuilder builder() {
    return new AggregateBuilder();
  }

  public AggregateBuilder toBuilder() {
    return new AggregateBuilder()
        .id(this.id)
        .type(this.type)
        .timestamp(this.timestamp)
        .payload(this.payload)
        .addMetadata(this.metadata)
        .aggregateId(this.aggregateId)
        .eventId(this.eventId)
        .version(this.version);
  }

  public static class AggregateBuilder {
    private String id;
    private Instant timestamp;
    private String type;
    private Object payload;
    private Metadata metadata = Metadata.builder().build();
    private String aggregateId;
    private String eventId;
    private long version = 0;

    private AggregateBuilder id(String id) {
      this.id = id;
      return this;
    }

    private AggregateBuilder type(String type) {
      this.type = type;
      return this;
    }

    public AggregateBuilder timestamp(Instant timestamp) {
      if (timestamp == null) {
        timestamp = Instant.now();
      }
      this.timestamp = timestamp;
      return this;
    }

    public AggregateBuilder payload(Object payload) {
      this.payload = payload;
      return this;
    }

    public AggregateBuilder addMetadata(String key, String value) {
      this.metadata = this.metadata.toBuilder()
          .entry(key, value)
          .build();
      return this;
    }

    public AggregateBuilder addMetadata(Metadata metadata) {
      Metadata.MetadataBuilder builder = this.metadata.toBuilder();
      if (metadata != null) {
        metadata.getEntries().forEach(builder::entry);
      }
      this.metadata = builder.build();
      return this;
    }

    public AggregateBuilder removeMetadata(String key) {
      Map<String, String> map = new HashMap<>(this.metadata.getEntries());
      map.keySet().removeIf(k -> k.equals(key));
      this.metadata = this.metadata.toBuilder()
          .clearEntries()
          .entries(map)
          .build();
      return this;
    }

    private AggregateBuilder aggregateId(String aggregateId) {
      this.aggregateId = aggregateId;
      return this;
    }

    public AggregateBuilder eventId(String eventId) {
      this.eventId = eventId;
      return this;
    }

    public AggregateBuilder version(long version) {
      this.version = version;
      return this;
    }

    public Aggregate build() {
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

      return new Aggregate(this);
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

  @Transient
  public int getSnapshotThreshold() {
    return Optional.ofNullable(getPayload())
        .map(Object::getClass)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, EnableSnapshotting.class))
        .map(EnableSnapshotting::threshold)
        .filter(threshold -> threshold > 0)
        .orElse(0);
  }

  @Transient
  public boolean deleteEvents() {
    return Optional.ofNullable(getPayload())
        .map(Object::getClass)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, EnableSnapshotting.class))
        .map(EnableSnapshotting::deleteEvents)
        .orElse(false);
  }
}
