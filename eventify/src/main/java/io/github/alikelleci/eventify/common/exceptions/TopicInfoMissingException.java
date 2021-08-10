package io.github.alikelleci.eventify.common.exceptions;

public class TopicInfoMissingException extends RuntimeException {

  public TopicInfoMissingException(String message) {
    super(message);
  }

  public TopicInfoMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}
