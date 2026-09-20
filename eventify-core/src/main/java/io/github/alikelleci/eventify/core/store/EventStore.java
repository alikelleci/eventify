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
  /** Present only on the command-processing thread. Query repositories are deliberately read-only. */
  private final KeyValueStore<String, Event> writable;

  public EventStore(ReadOnlyKeyValueStore<String, Event> store) {
    this.readable = store;
    this.writable = store instanceof KeyValueStore<String, Event> keyValueStore ? keyValueStore : null;
  }

  /** The event with this sequence, or {@code null} when the aggregate has none there. */
  public Event get(String aggregateType, String aggregateId, long sequence) {
    return sequence < 1 ? null : readable.get(StoreKeys.of(aggregateType, aggregateId, sequence));
  }

  /** All events of one aggregate, oldest first. The caller closes the iterator. */
  public EventIterator events(String aggregateType, String aggregateId) {
    return events(aggregateType, aggregateId, 1, Long.MAX_VALUE);
  }

  /**
   * Events in the inclusive sequence range, oldest first. Start at snapshot version + 1 to skip its event.
   * The caller closes the iterator, also after a partial read or a failure. An inverted range is empty.
   * @throws IllegalArgumentException when either sequence is below 1
   */
  public EventIterator events(String aggregateType, String aggregateId, long from, long to) {
    requireSequences(aggregateType, aggregateId, from, to);
    if (to < from) {
      return new EventIterator(null);
    }
    return new EventIterator(readable.range(StoreKeys.of(aggregateType, aggregateId, from), StoreKeys.of(aggregateType, aggregateId, to)));
  }

  /**
   * Events in the inclusive sequence range, newest first; from is the higher sequence. The caller closes the iterator.
   * @throws IllegalArgumentException when either sequence is below 1
   */
  public EventIterator eventsNewestFirst(String aggregateType, String aggregateId, long from, long to) {
    requireSequences(aggregateType, aggregateId, to, from);
    if (from < to) {
      return new EventIterator(null);
    }
    return new EventIterator(readable.reverseRange(StoreKeys.of(aggregateType, aggregateId, to), StoreKeys.of(aggregateType, aggregateId, from)));
  }

  /**
   * Appends events under their aggregate type, id and sequence. Only command processing may call this, within its
   * transaction: if an append fails after earlier appends, the processor must abort all of them.
   * @throws IllegalStateException when a sequence is already taken or the store is read-only
   */
  public void save(List<Event> events) {
    KeyValueStore<String, Event> store = writable();
    for (Event event : events) {
      Event taken = store.putIfAbsent(StoreKeys.of(event.getAggregateType(), event.getAggregateId(), event.getSequence()), event);
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
        StoreKeys.of(aggregateType, aggregateId, version - 1))) {
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
      throw new IllegalArgumentException("Cannot read the events of aggregate " + aggregateType + " " + aggregateId + " from sequence " + from
          + " to sequence " + to + ": a sequence starts at 1.");
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
