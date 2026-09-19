package io.github.alikelleci.eventify.core.store.exception;

import io.github.alikelleci.eventify.core.EventifyException;

/**
 * Writing to the event store failed. Not the failure of the command whose events were being written: the events of a
 * command are stored together or not at all, so the application stops the task instead of answering the command, and
 * exactly-once throws away everything that was written for it.
 */
public class EventStoreException extends EventifyException {

  public EventStoreException(String message, Throwable cause) {
    super(message, cause);
  }
}
