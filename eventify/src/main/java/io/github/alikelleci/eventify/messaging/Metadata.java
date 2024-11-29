package io.github.alikelleci.eventify.messaging;

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

  protected Metadata() {
    this.entries = new HashMap<>();
  }

  protected Metadata(Map<String, String> entries) {
    this.entries = entries;
  }

  @Override
  public String toString() {
    return entries.toString();
  }

  public Metadata addAll(Metadata metadata) {
    if (metadata != null) {
      this.entries.putAll(new HashMap<>(metadata));
    }
    return this;
  }

  public Metadata add(String key, String value) {
    this.entries.put(key, value);
    return this;
  }

  public Metadata remove(String key) {
    this.entries.remove(key);
    return this;
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

    public MetadataBuilder addAll(Metadata metadata) {
      if (metadata != null) {
        this.entries.putAll(new HashMap<>(metadata));
      }
      return this;
    }

    public MetadataBuilder add(String key, String value) {
      this.entries.put(key, value);
      return this;
    }

    public MetadataBuilder remove(String key) {
      this.entries.remove(key);
      return this;
    }

    public Metadata build() {
      return new Metadata(this.entries);
    }
  }

}
