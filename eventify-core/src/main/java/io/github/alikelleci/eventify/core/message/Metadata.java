package io.github.alikelleci.eventify.core.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.experimental.Delegate;

import java.util.HashMap;
import java.util.Map;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;
import static io.github.alikelleci.eventify.core.message.MetadataKeys.CORRELATION_ID;

@EqualsAndHashCode
public class Metadata implements Map<String, String> {

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


  @JsonIgnore
  public String getCorrelationId() {
    return this.entries.get(CORRELATION_ID);
  }

  @JsonIgnore
  public String getCausationId() {
    return this.entries.get(CAUSATION_ID);
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
      return new Metadata(this.entries);
    }
  }

}
