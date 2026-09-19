package io.github.alikelleci.eventify.core.message.exception;

import io.github.alikelleci.eventify.core.EventifyException;

public class AggregateIdMissingException extends EventifyException {

  public AggregateIdMissingException(String message) {
    super(message);
  }

  public AggregateIdMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}
