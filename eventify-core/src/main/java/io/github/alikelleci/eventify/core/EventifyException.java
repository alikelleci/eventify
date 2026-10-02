package io.github.alikelleci.eventify.core;

/** Base exception for Eventify failures. */
public class EventifyException extends RuntimeException {

  public EventifyException(String message) {
    super(message);
  }

  public EventifyException(String message, Throwable cause) {
    super(message, cause);
  }
}
