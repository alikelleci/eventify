package io.github.alikelleci.eventify.core.aggregate.exception;

import io.github.alikelleci.eventify.core.EventifyException;

/** The snapshot can't be used, and the events before it were deleted ({@code deleteEvents = true}). */
public class SnapshotOutdatedException extends EventifyException {

  public SnapshotOutdatedException(String message) {
    super(message);
  }
}
