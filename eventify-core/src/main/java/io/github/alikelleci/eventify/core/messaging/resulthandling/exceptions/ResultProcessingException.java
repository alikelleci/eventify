package io.github.alikelleci.eventify.core.messaging.resulthandling.exceptions;

public class ResultProcessingException extends RuntimeException {

  public ResultProcessingException(String message) {
    super(message);
  }

  public ResultProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}
