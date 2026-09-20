package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.EventStore;
import io.github.alikelleci.eventify.core.store.StoreKeys;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.NoSuchElementException;

/**
 * Reads the event store, keyed by {@link StoreKeys}. The events of one aggregate are in one key range, and nothing
 * else is, in the order of their sequence: the order they were handled in. {@link WritableEventStore} also writes it.
 */
public class ReadableEventStore implements EventStore {

  private final ReadOnlyKeyValueStore<String, Event> store;
  private final String aggregateType;

  public ReadableEventStore(ReadOnlyKeyValueStore<String, Event> store, String aggregateType) {
    this.store = store;
    this.aggregateType = aggregateType;
  }

  /** The aggregate this store holds, as its {@code @AggregateRoot} names it. */
  public String aggregateType() {
    return aggregateType;
  }

  @Override
  public Event get(String aggregateId, long sequence) {
    return sequence < 1 ? null : store.get(StoreKeys.of(aggregateType, aggregateId, sequence));
  }

  /**
   * The sequence of the aggregate's last stored event; 0 when it has none. Deleting events at a snapshot keeps the
   * snapshot's event and the ones after it, so the last event is never deleted.
   *
   * <p>Reads one entry, however many events the aggregate has: the iterator is lazy, and the first key it gives is
   * the answer. The sequence comes from the key: the store orders and checks the events by it.
   */
  @Override
  public long lastSequence(String aggregateId) {
    // reverseRange starts at the end of the aggregate's key range: its first key is the aggregate's last event.
    try (KeyValueIterator<String, Event> keys = store.reverseRange(StoreKeys.first(aggregateType, aggregateId), StoreKeys.last(aggregateType, aggregateId))) {
      return keys.hasNext() ? StoreKeys.sequenceOf(aggregateType, aggregateId, keys.next().key) : 0;
    }
  }

  @Override
  public Events events(String aggregateId, long from, long to) {
    requireSequences(aggregateId, from, to);
    if (to < from) {
      return new OfAggregate(null); // an empty range: its start would come after its end
    }
    return new OfAggregate(store.range(StoreKeys.of(aggregateType, aggregateId, from), StoreKeys.of(aggregateType, aggregateId, to)));
  }

  @Override
  public Events eventsNewestFirst(String aggregateId, long from, long to) {
    requireSequences(aggregateId, to, from);
    if (from < to) {
      return new OfAggregate(null);
    }
    return new OfAggregate(store.reverseRange(StoreKeys.of(aggregateType, aggregateId, to), StoreKeys.of(aggregateType, aggregateId, from)));
  }

  private static void requireSequences(String aggregateId, long from, long to) {
    if (from < 1 || to < 1) {
      throw new IllegalArgumentException("Cannot read the events of aggregate '" + aggregateId + "' from sequence " + from + " to sequence " + to + ": a sequence starts at 1.");
    }
  }

  /** The events in a key range, which are an aggregate's own: see {@link StoreKeys}. */
  private static class OfAggregate implements Events {

    /** {@code null} for no events. */
    private final KeyValueIterator<String, Event> iterator;

    OfAggregate(KeyValueIterator<String, Event> iterator) {
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
