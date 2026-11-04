package io.github.alikelleci.eventify.core.command;

import java.time.Instant;

/**
 * A command an event handler returns to deliver at deliverAt, not earlier: a payload or a {@link Command}. Take the
 * time from the event, not from the clock, so a replay schedules the same time.
 */
public record Scheduled(Object command, Instant deliverAt) {

  public Scheduled {
    if (command == null || deliverAt == null) {
      throw new IllegalArgumentException("A scheduled command needs a command and a delivery time.");
    }
    if (deliverAt.toEpochMilli() < 0) {
      throw new IllegalArgumentException("A delivery time can't be before 1970, not " + deliverAt + ".");
    }
    if (command instanceof Scheduled) {
      throw new IllegalArgumentException("A scheduled command can't be scheduled again.");
    }
  }

  public static Scheduled of(Object command, Instant deliverAt) {
    return new Scheduled(command, deliverAt);
  }
}
