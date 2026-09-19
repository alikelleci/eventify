package io.github.alikelleci.eventify.core.message.exception;

import io.github.alikelleci.eventify.core.EventifyException;

public class PayloadMissingException extends EventifyException {

  public PayloadMissingException(String message) {
    super(message);
  }

  public PayloadMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}
