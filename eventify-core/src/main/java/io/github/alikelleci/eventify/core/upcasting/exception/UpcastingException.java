package io.github.alikelleci.eventify.core.upcasting.exception;

import io.github.alikelleci.eventify.core.EventifyException;

public class UpcastingException extends EventifyException {

  public UpcastingException(String message) {
    super(message);
  }

  public UpcastingException(String message, Throwable cause) {
    super(message, cause);
  }
}
