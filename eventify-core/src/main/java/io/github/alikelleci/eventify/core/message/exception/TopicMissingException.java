package io.github.alikelleci.eventify.core.message.exception;

public class TopicMissingException extends RuntimeException {

  public TopicMissingException(String message) {
    super(message);
  }

  public TopicMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}
