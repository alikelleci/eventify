package io.github.alikelleci.eventify.core.message.exception;

public class PayloadMissingException extends RuntimeException {

  public PayloadMissingException(String message) {
    super(message);
  }

  public PayloadMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}
