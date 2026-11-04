package io.github.alikelleci.eventify.core.command;

import java.time.Instant;
import java.util.Objects;

/** A scheduled command, to cancel it with. Can be kept as JSON, for example in an aggregate's state. */
public record ScheduleToken(String topic, String aggregateId, String commandId, Instant deliverAt) {

  public ScheduleToken {
    Objects.requireNonNull(topic, "topic");
    Objects.requireNonNull(aggregateId, "aggregateId");
    Objects.requireNonNull(commandId, "commandId");
    Objects.requireNonNull(deliverAt, "deliverAt");
  }
}
