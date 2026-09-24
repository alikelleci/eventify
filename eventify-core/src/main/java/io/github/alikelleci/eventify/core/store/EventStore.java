package io.github.alikelleci.eventify.core.store;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.List;
import java.util.NoSuchElementException;

/** The events Eventify stores for all its aggregate types. */
public final class EventStore {

  private final ReadOnlyKeyValueStore<String, Event> readable;
  /** Only on the command-processing thread; queries are read-only. */
  private final KeyValueStore<String, Event> writable;

  public EventStore(ReadOnlyKeyValueStore<String, Event> store) {
    this.readable = store;
    this.writable = store instanceof KeyValueStore<String, Event> keyValueStore ? keyValueStore : null;
  }

  /** The event with this sequence, or {@code null} when the aggregate has none there. */
  public Event get(String aggregateType, String aggregateId, long sequence) {
    return sequence < 1 ? null : readable.get(StoreKeys.event(aggregateType, aggregateId, sequence));
  }

  /** All events of one aggregate, oldest first. The caller closes the iterator. */
  public EventIterator events(String aggregateType, String aggregateId) {
    return events(aggregateType, aggregateId, 1, Long.MAX_VALUE);
  }

  /** Events in the inclusive range, oldest first; empty when inverted. The caller closes the iterator. */
  public EventIterator events(String aggregateType, String aggregateId, long from, long to) {
    requireSequences(aggregateType, aggregateId, from, to);
    if (to < from) {
      return new EventIterator(null);
    }
    return new EventIterator(readable.range(StoreKeys.event(aggregateType, aggregateId, from), StoreKeys.event(aggregateType, aggregateId, to)));
  }

  /** Events in the inclusive range, newest first ({@code from} is the higher). The caller closes the iterator. */
  public EventIterator eventsNewestFirst(String aggregateType, String aggregateId, long from, long to) {
    requireSequences(aggregateType, aggregateId, from, to);
    if (from < to) {
      return new EventIterator(null);
    }
    return new EventIterator(readable.reverseRange(StoreKeys.event(aggregateType, aggregateId, to), StoreKeys.event(aggregateType, aggregateId, from)));
  }

  /** Appends events; command processing only, within its transaction. Throws when a sequence is taken. */
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

  /** Deletes events before a snapshot. The snapshot event itself remains available for history queries. */
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

  /** A closeable iterator over stored events. Closing releases the underlying Kafka Streams iterator. */
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
