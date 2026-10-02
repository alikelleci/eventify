package io.github.alikelleci.eventify.core.message;

/** Metadata keys set by Eventify; the "$" avoids clashes with application keys. */
public final class MetadataKeys {

  /** The flow a message belongs to; set on a new command and passed on to its events. */
  public static final String CORRELATION_ID = "$correlationId";
  /** On an event: the id of the command that produced it. */
  public static final String CAUSATION_ID = "$causationId";

  private MetadataKeys() {
  }
}
