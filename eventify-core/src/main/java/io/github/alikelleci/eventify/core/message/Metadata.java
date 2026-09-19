package io.github.alikelleci.eventify.core.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.experimental.Delegate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;
import static io.github.alikelleci.eventify.core.message.MetadataKeys.CORRELATION_ID;

/**
 * What a message carries besides its payload: where it came from, and whatever the application adds.
 *
 * <p>A Metadata never changes. {@link #with} and {@link #withDefault} give a new one with the entry, and the methods
 * of {@link Map} that would change it throw: the metadata of a message stays the one it was made with, also when the
 * same Metadata is given to more than one message.
 */
@EqualsAndHashCode
public class Metadata implements Map<String, String> {

  @Delegate
  private final Map<String, String> entries;

  @JsonCreator
  private Metadata(Map<String, String> entries) {
    this.entries = Collections.unmodifiableMap(new HashMap<>(entries));
  }

  @Override
  public String toString() {
    return entries.toString();
  }

  /** This metadata with the entry; the value replaces the one that was there. */
  public Metadata with(String key, String value) {
    return builder().putAll(this).put(key, value).build();
  }

  /** This metadata with the entry, when it has no value for that key yet. */
  public Metadata withDefault(String key, String value) {
    return containsKey(key) ? this : with(key, value);
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
