package io.github.alikelleci.eventify.core.store;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.List;
import java.util.NoSuchElementException;

/** Stores events by aggregate and sequence. */
public final class EventStore {

  private final ReadOnlyKeyValueStore<String, Event> readable;
  /** Available only while processing commands. */
  private final KeyValueStore<String, Event> writable;

  public EventStore(ReadOnlyKeyValueStore<String, Event> store) {
    this.readable = store;
    this.writable = store instanceof KeyValueStore<String, Event> keyValueStore ? keyValueStore : null;
  }

  /** Returns the event at a sequence, or {@code null}. */
  public Event get(String aggregateType, String aggregateId, long sequence) {
    return sequence < 1 ? null : readable.get(StoreKeys.event(aggregateType, aggregateId, sequence));
  }

  /** Returns all events oldest first. Close the iterator. */
  public EventIterator events(String aggregateType, String aggregateId) {
    return events(aggregateType, aggregateId, 1, Long.MAX_VALUE);
  }

  /** Returns an inclusive range oldest first. Close the iterator. */
  public EventIterator events(String aggregateType, String aggregateId, long from, long to) {
    requireSequences(aggregateType, aggregateId, from, to);
    if (to < from) {
      return new EventIterator(null);
    }
    return new EventIterator(readable.range(StoreKeys.event(aggregateType, aggregateId, from), StoreKeys.event(aggregateType, aggregateId, to)));
  }

  /** Returns an inclusive range newest first. Close the iterator. */
  public EventIterator eventsNewestFirst(String aggregateType, String aggregateId, long from, long to) {
    requireSequences(aggregateType, aggregateId, from, to);
    if (from < to) {
      return new EventIterator(null);
    }
    return new EventIterator(readable.reverseRange(StoreKeys.event(aggregateType, aggregateId, to), StoreKeys.event(aggregateType, aggregateId, from)));
  }

  /** Appends events and rejects an occupied sequence. */
  public void save(List<Event> events) {
    KeyValueStore<String, Event> store = writable();
    for (Event event : events) {
      Event taken = store.putIfAbsent(StoreKeys.event(event.getAggregateType(), event.getAggregateId(), event.getSequence()), event);
      if (taken != null) {
        throw new IllegalStateException("Aggregate " + event.getAggregateType() + " " + event.getAggregateId() + " already has an event with sequence "
            + event.getSequence() + ": event " + taken.getId() + ".");
      }
    }
  }

  /** Deletes events before a snapshot and retains the snapshot event. */
  public long deleteBefore(String aggregateType, String aggregateId, long version) {
    if (version <= 1) {
      return 0;
    }
    long deleted = 0;
    KeyValueStore<String, Event> store = writable();
    try (KeyValueIterator<String, Event> iterator = store.range(StoreKeys.first(aggregateType, aggregateId),
        StoreKeys.event(aggregateType, aggregateId, version - 1))) {
      while (iterator.hasNext()) {
        store.delete(iterator.next().key);
        deleted++;
      }
    }
    return deleted;
  }

  private KeyValueStore<String, Event> writable() {
    if (writable == null) {
      throw new IllegalStateException("This EventStore is read-only.");
    }
    return writable;
  }

  private static void requireSequences(String aggregateType, String aggregateId, long from, long to) {
    if (from < 1 || to < 1) {
      throw new IllegalArgumentException("Invalid range " + from + ".." + to + " for " + aggregateType + " " + aggregateId + ": sequences start at 1.");
    }
  }

  /** Closeable iterator over stored events. */
  public static final class EventIterator implements java.util.Iterator<Event>, AutoCloseable {
    private final KeyValueIterator<String, Event> iterator;

    EventIterator(KeyValueIterator<String, Event> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator != null && iterator.hasNext();
    }

    @Override
    public Event next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return iterator.next().value;
    }

    @Override
    public void close() {
      if (iterator != null) {
        iterator.close();
      }
    }
  }
}
