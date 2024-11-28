package io.github.alikelleci.eventify.messaging;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Delegate;

import java.beans.Transient;
import java.util.HashMap;
import java.util.Map;

@Value
public class Metadata implements Map<String, String> {
  public static final String CORRELATION_ID = "$correlationId";
  public static final String REPLY_TO = "$replyTo";
  public static final String RESULT = "$result";
  public static final String CAUSE = "$cause";

  @Delegate
  Map<String, String> entries;

  protected Metadata() {
    this.entries = new HashMap<>();
  }

  @Builder(toBuilder = true)
  protected Metadata(@Singular Map<String, String> entries) {
    this.entries = entries;
  }

  @Transient
  public String getCorrelationId() {
    return this.entries.get(CORRELATION_ID);
  }

}
