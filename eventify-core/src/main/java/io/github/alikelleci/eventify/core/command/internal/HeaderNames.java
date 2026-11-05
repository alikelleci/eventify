package io.github.alikelleci.eventify.core.command.internal;

/** The Kafka record headers Eventify sets and reads. */
public final class HeaderNames {

  /** On a command record: the topic its sender waits on. A header, so it isn't part of the command or its events. */
  public static final String REPLY_TO = "eventify-reply-to";

  /** On a scheduled command record and its cancel record: when to deliver it, in epoch milliseconds. */
  public static final String DELIVER_AT = "eventify-deliver-at";

  /** On a cancel record, which has no value: the id of the scheduled command to cancel. */
  public static final String CANCEL = "eventify-cancel";

  private HeaderNames() {
  }
}
