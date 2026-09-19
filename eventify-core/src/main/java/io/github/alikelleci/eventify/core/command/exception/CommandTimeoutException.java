package io.github.alikelleci.eventify.core.command.exception;

import io.github.alikelleci.eventify.core.EventifyException;

/** No result arrived for a command in time. The command may still be handled: only the wait for its result ended. */
public class CommandTimeoutException extends EventifyException {

  public CommandTimeoutException(String message) {
    super(message);
  }

  public CommandTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
