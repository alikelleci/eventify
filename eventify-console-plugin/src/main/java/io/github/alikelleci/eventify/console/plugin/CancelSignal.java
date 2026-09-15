package io.github.alikelleci.eventify.console.plugin;

import java.util.ArrayList;
import java.util.List;

/**
 * Tells a query that nobody waits for its answer anymore, e.g. because someone refreshed the page. Every request has
 * its own signal, so cancelling one never affects another.
 */
public final class CancelSignal {

  private boolean cancelled;
  private final List<Runnable> actions = new ArrayList<>();

  public synchronized boolean isCancelled() {
    return cancelled;
  }

  /**
   * Runs the action when the request is cancelled, or right away if it already is. Close the returned registration
   * once the action no longer applies, e.g. before closing the resource the action would stop.
   */
  public synchronized AutoCloseable onCancel(Runnable action) {
    if (cancelled) {
      action.run();
    } else {
      actions.add(action);
    }
    return () -> {
      synchronized (this) {
        actions.remove(action);
      }
    };
  }

  synchronized void cancel() {
    if (cancelled) {
      return;
    }
    cancelled = true;
    actions.forEach(Runnable::run);
    actions.clear();
  }
}
