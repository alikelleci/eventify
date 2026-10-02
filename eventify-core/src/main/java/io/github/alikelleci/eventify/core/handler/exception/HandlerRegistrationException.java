package io.github.alikelleci.eventify.core.handler.exception;

import io.github.alikelleci.eventify.core.EventifyException;

/** A handler that can't be registered, e.g. a second {@code @CommandHandler} for a command that already has one. */
public class HandlerRegistrationException extends EventifyException {

  public HandlerRegistrationException(String message) {
    super(message);
  }

  public HandlerRegistrationException(String message, Throwable cause) {
    super(message, cause);
  }
}
