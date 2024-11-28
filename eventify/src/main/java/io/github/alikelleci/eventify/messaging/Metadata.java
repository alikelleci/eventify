package io.github.alikelleci.eventify.messaging;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Delegate;

import java.beans.Transient;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@ToString
@EqualsAndHashCode
public class Metadata implements Map<String, String> {
  public static final String ID = "$id";
  public static final String TIMESTAMP = "$timestamp";
  public static final String AGGREGATE_ID = "$aggregateId";
  public static final String CORRELATION_ID = "$correlationId";
  public static final String REPLY_TO = "$replyTo";
  public static final String REVISION = "$revision";
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


  @Transient
  public String getId() {
    return this.entries.get(ID);
  }

  @Transient
  public Instant getTimestamp() {
    return Instant.parse(this.entries.get(TIMESTAMP));
  }


}
