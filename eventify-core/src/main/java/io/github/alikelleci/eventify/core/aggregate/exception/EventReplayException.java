package io.github.alikelleci.eventify.core.aggregate.exception;

import io.github.alikelleci.eventify.core.EventifyException;

/**
 * The stored events of an aggregate can't be replayed: one of them can no longer be read as its class, or they are
 * incomplete or out of order (a sequence is missing, or doesn't fit its place). Not a failure of a handler: the stored
 * data has to be fixed, and until then every replay of the aggregate fails with this.
 */
public class EventReplayException extends EventifyException {

  public EventReplayException(String message) {
    super(message);
  }
}
