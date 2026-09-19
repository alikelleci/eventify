package io.github.alikelleci.eventify.core.kafka;

/** The Kafka record headers Eventify sets and reads. */
public final class HeaderNames {

  /**
   * On a command record: the topic its sender waits for the result on. A header, not metadata: it says where to send
   * the answer, which is nothing about the command itself. It stays on the record it was set on, so the command is
   * the same whether or not anyone waits for its result, and the events it produces carry nothing of it.
   */
  public static final String REPLY_TO = "eventify-reply-to";

  private HeaderNames() {
  }
}
