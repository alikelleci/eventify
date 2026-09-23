package io.github.alikelleci.eventify.core.kafka;

/** The Kafka record headers Eventify sets and reads. */
public final class HeaderNames {

  /** On a command record: the topic its sender waits on. A header, so it isn't part of the command or its events. */
  public static final String REPLY_TO = "eventify-reply-to";

  private HeaderNames() {
  }
}
