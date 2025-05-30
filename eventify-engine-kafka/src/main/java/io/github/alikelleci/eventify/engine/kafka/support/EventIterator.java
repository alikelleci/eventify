package io.github.alikelleci.eventify.engine.kafka.support;

import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.support.AutoCloseableIterator;
import org.apache.kafka.streams.state.KeyValueIterator;

public class EventIterator implements AutoCloseableIterator<Event> {

  private final KeyValueIterator<String, Event> delegate;
  private boolean closed = false;

  public EventIterator(KeyValueIterator<String, Event> delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public Event next() {
    return delegate.next().value;
  }

  @Override
  public void close() {
    if (!closed) {
      delegate.close();
      closed = true;
    }
  }
}
