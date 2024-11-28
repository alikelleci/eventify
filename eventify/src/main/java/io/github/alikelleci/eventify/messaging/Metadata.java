package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.beans.Transient;
import java.util.Collections;
import java.util.Map;

@Value
@Builder(toBuilder = true)
public class Metadata  {
  public static final String CORRELATION_ID = "$correlationId";
  public static final String REPLY_TO = "$replyTo";
  public static final String RESULT = "$result";
  public static final String CAUSE = "$cause";

  @Singular
  Map<String, String> entries;

  @JsonAnyGetter
  public Map<String, String> getEntries() {
    return entries;
  }

  @JsonCreator
  private Metadata(Map<String, String> entries) {
    this.entries = Collections.unmodifiableMap(entries);
  }

  @Override
  public String toString() {
    return entries.toString();
  }

  public String get(String key) {
    return this.entries.get(key);
  }

  public void put(String key, String value) {
    this.entries.put(key, value);
  }

  public void remove(String key) {
    this.entries.remove(key);
  }



  @Transient
  public String getCorrelationId() {
    return this.entries.get(CORRELATION_ID);
  }

}
