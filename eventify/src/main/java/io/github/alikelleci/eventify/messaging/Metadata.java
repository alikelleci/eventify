package io.github.alikelleci.eventify.messaging;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Delegate;

import java.beans.Transient;
import java.util.Map;

@Value
@Builder(toBuilder = true)
public class Metadata implements Map<String,String> {
  public static final String CORRELATION_ID = "$correlationId";
  public static final String REPLY_TO = "$replyTo";
  public static final String RESULT = "$result";
  public static final String CAUSE = "$cause";

  @Delegate
  @Singular
  Map<String, String> entries;

  @Transient
  public String getCorrelationId() {
    return this.entries.get(CORRELATION_ID);
  }

}
