package io.github.alikelleci.eventify.core.message.exception;

public class AggregateIdMissingException extends RuntimeException {

  public AggregateIdMissingException(String message) {
    super(message);
  }

  public AggregateIdMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}
