package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.util.IdUtils;
import lombok.Value;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Value
public class Event implements Message {
  String id;
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  String aggregateId;
  int revision;

  private Event() {
    this.id = null;
    this.timestamp = null;
    this.type = null;
    this.payload = null;
    this.metadata = null;
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

  public EventBuilder toBuilder() {
    return new EventBuilder()
        .id(this.id)
        .type(this.type)
        .timestamp(this.timestamp)
        .payload(this.payload)
        .addMetadata(this.metadata)
        .aggregateId(this.aggregateId)
        .revision(this.revision);
  }

  public static EventBuilder builder() {
    return new EventBuilder();
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

    public EventBuilder addMetadata(String key, String value) {
      this.metadata = this.metadata.toBuilder()
          .entry(key, value)
          .build();
      return this;
    }

    public EventBuilder addMetadata(Metadata metadata) {
      Metadata.MetadataBuilder builder = this.metadata.toBuilder();
      if (metadata != null) {
        metadata.getEntries().forEach(builder::entry);
      }
      this.metadata = builder.build();
      return this;
    }

    public EventBuilder removeMetadata(String key) {
      Map<String, String> map = new HashMap<>(this.metadata.getEntries());
      map.keySet().removeIf(k -> k.equals(key));
      this.metadata = this.metadata.toBuilder()
          .clearEntries()
          .entries(map)
          .build();
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
      this.type = payload.getClass().getSimpleName();
      this.aggregateId = IdUtils.getAggregateId(this.payload);

      if (this.timestamp == null) {
        this.timestamp = Instant.now();
      }
      if (this.id == null) {
        this.id = IdUtils.createCompoundKey(this.aggregateId, this.timestamp);
      }
      return new Event(this);
    }
  }
}
