package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.EqualsAndHashCode;
import lombok.experimental.Delegate;

import java.beans.Transient;
import java.util.HashMap;
import java.util.Map;

@EqualsAndHashCode
public class Metadata implements Map<String, String> {
  public static final String CORRELATION_ID = "$correlationId";
  public static final String REPLY_TO = "$replyTo";
  public static final String RESULT = "$result";
  public static final String CAUSE = "$cause";

  @Delegate
  private final Map<String, String> entries;

  private Metadata() {
    this.entries = new HashMap<>();
  }

  @JsonCreator
  private Metadata(Map<String, String> entries) {
    this.entries = new HashMap<>(entries);
  }

  @Override
  public String toString() {
    return entries.toString();
  }


  @Transient
  public String getCorrelationId() {
    return this.entries.get(CORRELATION_ID);
  }

  public static MetadataBuilder builder() {
    return new MetadataBuilder();
  }

  public static class MetadataBuilder {

    private final Map<String, String> entries = new HashMap<>();

    public MetadataBuilder put(String key, String value) {
      this.entries.put(key, value);
      return this;
    }

    public MetadataBuilder putAll(Map<String, String> metadata) {
      if (metadata != null) {
        this.entries.putAll(metadata);
      }
      return this;
    }

    public MetadataBuilder putAll(Metadata metadata) {
      if (metadata != null) {
        this.entries.putAll(metadata);
      }
      return this;
    }

    public Metadata build() {
      this.entries.remove(RESULT);
      this.entries.remove(CAUSE);

      return new Metadata(this.entries);
    }
  }

}
