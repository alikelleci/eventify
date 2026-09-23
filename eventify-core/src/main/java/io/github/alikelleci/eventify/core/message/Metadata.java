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
 * What a message carries besides its payload. Immutable: {@link #with} returns a new one and mutating Map methods
 * throw, e.g. {@code Metadata.of("tenant", "acme").with("user", "ada")}.
 */
@EqualsAndHashCode
public class Metadata implements Map<String, String> {

  /** No metadata at all. */
  public static final Metadata EMPTY = new Metadata(Map.of());

  @Delegate
  private final Map<String, String> entries;

  @JsonCreator
  private Metadata(Map<String, String> entries) {
    this.entries = Collections.unmodifiableMap(new HashMap<>(entries));
  }

  /** Metadata with this one entry. */
  public static Metadata of(String key, String value) {
    return EMPTY.with(key, value);
  }

  /** Metadata with these entries; {@link #EMPTY} when there are none. */
  public static Metadata of(Map<String, String> entries) {
    return EMPTY.with(entries);
  }

  /** This metadata with the entry; the value replaces the one that was there. */
  public Metadata with(String key, String value) {
    Map<String, String> copy = new HashMap<>(entries);
    copy.put(key, value);
    return new Metadata(copy);
  }

  /** This metadata with the entries added, replacing existing values. */
  public Metadata with(Map<String, String> entries) {
    if (entries == null || entries.isEmpty()) {
      return this;
    }
    Map<String, String> copy = new HashMap<>(this.entries);
    copy.putAll(entries);
    return new Metadata(copy);
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

  @Override
  public String toString() {
    return entries.toString();
  }
}
