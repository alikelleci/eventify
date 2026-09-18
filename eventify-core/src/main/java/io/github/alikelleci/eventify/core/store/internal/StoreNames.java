package io.github.alikelleci.eventify.core.store.internal;

/** The names of Eventify's state stores. Their changelog topics are named after them too. */
public final class StoreNames {

  public static final String EVENT_STORE = "event-store";
  public static final String SNAPSHOT_STORE = "snapshot-store";

  private StoreNames() {
  }
}
