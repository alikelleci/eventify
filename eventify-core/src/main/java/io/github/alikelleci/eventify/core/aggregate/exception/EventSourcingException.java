package io.github.alikelleci.eventify.core.aggregate.exception;

import io.github.alikelleci.eventify.core.EventifyException;

public class EventSourcingException extends EventifyException {

  public EventSourcingException(String message) {
    super(message);
  }

  public EventSourcingException(String message, Throwable cause) {
    super(message, cause);
  }
}
