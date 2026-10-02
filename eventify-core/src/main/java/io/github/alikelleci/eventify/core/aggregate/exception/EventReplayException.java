package io.github.alikelleci.eventify.core.aggregate.exception;

import io.github.alikelleci.eventify.core.EventifyException;

/** The stored events can't be replayed (unreadable class, gap or wrong order): the data has to be fixed. */
public class EventReplayException extends EventifyException {

  public EventReplayException(String message) {
    super(message);
  }
}
