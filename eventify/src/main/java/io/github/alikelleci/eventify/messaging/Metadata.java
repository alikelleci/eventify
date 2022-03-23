package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.ToString;
import lombok.experimental.Delegate;
import org.apache.commons.lang3.StringUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@ToString
public class Metadata implements Map<String, String> {
  public static final String ID = "$id";
  public static final String TIMESTAMP = "$timestamp";
  public static final String CORRELATION_ID = "$correlationId";
  public static final String REPLY_TO = "$replyTo";
  public static final String REVISION = "$revision";
  public static final String RESULT = "$result";
  public static final String CAUSE = "$cause";

  @Delegate
  private Map<String, String> entries;

  public Metadata() {
    this.entries = new HashMap<>();
  }

  public Metadata(Metadata metadata) {
    this.entries = new HashMap<>(metadata);
  }

  @Transient
  public Metadata filter() {
    entries.keySet().removeIf(key -> StringUtils.startsWithIgnoreCase(key, "$"));
    return this;
  }

  public Metadata addAll(Metadata metadata) {
    this.entries.putAll(new HashMap<>(metadata));
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

  @JsonIgnore
  public String getMessageId() {
    return this.entries.get(ID);
  }

  @JsonIgnore
  public Instant getTimestamp() {
    return Instant.parse(this.entries.get(TIMESTAMP));
  }


  public static class Builder {

  }
}
