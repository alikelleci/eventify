package io.github.alikelleci.eventify.core.aggregate.exception;

import io.github.alikelleci.eventify.core.EventifyException;

/**
 * An aggregate's snapshot can't be used, and the aggregate can't be rebuilt without it: the events before it were
 * deleted ({@code @EnableSnapshotting(deleteEvents = true)}).
 */
public class SnapshotOutdatedException extends EventifyException {

  public SnapshotOutdatedException(String message) {
    super(message);
  }
}
