package io.github.alikelleci.eventify.core;

/** What Eventify throws: catch this to catch every failure that comes from Eventify. */
public class EventifyException extends RuntimeException {

  public EventifyException(String message) {
    super(message);
  }

  public EventifyException(String message, Throwable cause) {
    super(message, cause);
  }
}
