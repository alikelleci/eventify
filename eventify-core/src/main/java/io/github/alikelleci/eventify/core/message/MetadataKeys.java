package io.github.alikelleci.eventify.core.message;

/** The metadata keys Eventify sets itself. They start with "$", so they don't clash with an application's own keys. */
public final class MetadataKeys {

  /** The flow a message belongs to: set on a command when it is created, and passed on to its events. */
  public static final String CORRELATION_ID = "$correlationId";
  /** On an event: the id of the command that produced it. */
  public static final String CAUSATION_ID = "$causationId";
  /** On a command: the topic its sender waits for the result on. */
  public static final String REPLY_TO = "$replyTo";

  private MetadataKeys() {
  }
}
