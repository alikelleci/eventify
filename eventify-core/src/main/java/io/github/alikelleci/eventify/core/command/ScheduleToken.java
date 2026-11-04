package io.github.alikelleci.eventify.core.command;

import java.time.Instant;

/** A scheduled command, to cancel it with. Can be kept as JSON, for example in an aggregate's state. */
public record ScheduleToken(String topic, String aggregateId, String commandId, Instant deliverAt) {

  public ScheduleToken {
    if (topic == null || aggregateId == null || commandId == null || deliverAt == null) {
      throw new IllegalArgumentException("A schedule token needs a topic, aggregate id, command id and delivery time.");
    }
  }
}
