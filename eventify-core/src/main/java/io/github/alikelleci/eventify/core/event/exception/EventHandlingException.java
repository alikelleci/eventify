package io.github.alikelleci.eventify.core.event.exception;

public class EventHandlingException extends RuntimeException {

  public EventHandlingException(String message) {
    super(message);
  }

  public EventHandlingException(String message, Throwable cause) {
    super(message, cause);
  }
}
