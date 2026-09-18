package io.github.alikelleci.eventify.core.aggregate.exception;

public class EventSourcingException extends RuntimeException {

  public EventSourcingException(String message) {
    super(message);
  }

  public EventSourcingException(String message, Throwable cause) {
    super(message, cause);
  }
}
