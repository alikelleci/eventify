package io.github.alikelleci.eventify.core.command.exception;

import io.github.alikelleci.eventify.core.EventifyException;

public class CommandExecutionException extends EventifyException {

  public CommandExecutionException(String message) {
    super(message);
  }

  public CommandExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
