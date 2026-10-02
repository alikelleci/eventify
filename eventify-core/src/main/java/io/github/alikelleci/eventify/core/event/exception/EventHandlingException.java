package io.github.alikelleci.eventify.core.event.exception;

import io.github.alikelleci.eventify.core.EventifyException;

public class EventHandlingException extends EventifyException {

  public EventHandlingException(String message) {
    super(message);
  }

  public EventHandlingException(String message, Throwable cause) {
    super(message, cause);
  }
}
