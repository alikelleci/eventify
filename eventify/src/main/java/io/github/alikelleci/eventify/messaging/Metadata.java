package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder(toBuilder = true)
public class Metadata {
  public static final String ID = "$id";
  public static final String CORRELATION_ID = "$correlationId";
  public static final String REVISION = "$revision";
  public static final String RESULT = "$result";
  public static final String CAUSE = "$cause";

  @Singular
  private Map<String, String> entries;

  @JsonIgnore
  private String messageId;
  @JsonIgnore
  private Instant timestamp;


  @Transient
  public Metadata filter() {
    Map<String, String> map = new HashMap<>(entries);
    map.keySet().removeIf(key -> StringUtils.startsWithIgnoreCase(key, "$"));

    return this.toBuilder()
        .clearEntries()
        .entries(map)
        .build();
  }

  public String get(String key) {
    return entries.get(key);
  }

  public Metadata add(String key, String value) {
    return this.toBuilder()
        .entry(key, value)
        .build();
  }
}
