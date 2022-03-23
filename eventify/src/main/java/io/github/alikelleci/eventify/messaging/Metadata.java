package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.ToString;
import lombok.experimental.Delegate;
import org.apache.commons.lang3.StringUtils;

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
  private Map<String, String> entries = new HashMap<>();

  public Metadata() {
  }

  public Metadata(Metadata metadata) {
    if (metadata != null) {
      this.entries.putAll(new HashMap<>(metadata));
    }
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

  @JsonIgnore
  protected Metadata filter() {
    entries.keySet().removeIf(key ->
        !StringUtils.equalsIgnoreCase(key, CORRELATION_ID) && StringUtils.startsWithIgnoreCase(key, "$"));
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

}
