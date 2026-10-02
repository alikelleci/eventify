package io.github.alikelleci.eventify.core.message.exception;

import io.github.alikelleci.eventify.core.EventifyException;

public class AggregateIdMismatchException extends EventifyException {

  public AggregateIdMismatchException(String message) {
    super(message);
  }

  public AggregateIdMismatchException(String message, Throwable cause) {
    super(message, cause);
  }
}
