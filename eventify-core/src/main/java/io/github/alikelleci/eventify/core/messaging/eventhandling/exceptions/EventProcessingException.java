package io.github.alikelleci.eventify.core.messaging.eventhandling.exceptions;

public class EventProcessingException extends RuntimeException {

  public EventProcessingException(String message) {
    super(message);
  }

  public EventProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}