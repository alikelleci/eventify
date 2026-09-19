package io.github.alikelleci.eventify.core.message.exception;

import io.github.alikelleci.eventify.core.EventifyException;

public class TopicMissingException extends EventifyException {

  public TopicMissingException(String message) {
    super(message);
  }

  public TopicMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}
