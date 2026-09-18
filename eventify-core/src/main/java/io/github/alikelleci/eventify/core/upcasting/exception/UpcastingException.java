package io.github.alikelleci.eventify.core.upcasting.exception;

public class UpcastingException extends RuntimeException {

  public UpcastingException(String message) {
    super(message);
  }

  public UpcastingException(String message, Throwable cause) {
    super(message, cause);
  }
}
