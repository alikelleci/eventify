package io.github.alikelleci.eventify.core.support;

import java.util.Iterator;

public interface AutoCloseableIterator<T> extends Iterator<T>, AutoCloseable {
  void close();
}
