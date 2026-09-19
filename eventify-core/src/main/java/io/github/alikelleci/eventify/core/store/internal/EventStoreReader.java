package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.ReadOnlyEventStore;
import io.github.alikelleci.eventify.core.store.StoreKeys;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.NoSuchElementException;

/**
 * Reads the event store, stored by {@link StoreKeys}: {@code aggregateId@sequence}. The events of one aggregate are in
 * one key range, in the order of their sequence: the order they were handled in. {@link EventStore} also writes it.
 */
public class EventStoreReader implements ReadOnlyEventStore {

  private final ReadOnlyKeyValueStore<String, Event> store;

  public EventStoreReader(ReadOnlyKeyValueStore<String, Event> store) {
    this.store = store;
  }

  @Override
  public Event get(String aggregateId, long sequence) {
    return sequence < 1 ? null : store.get(StoreKeys.of(aggregateId, sequence));
  }

  /**
   * The sequence of the aggregate's last stored event; 0 when it has none. Deleting events at a snapshot keeps the
   * snapshot's event and the ones after it, so the last event is never deleted.
   *
   * <p>Reads one entry, however many events the aggregate has: the iterator is lazy, and stops at the first key of
   * this aggregate. The sequence comes from the key: the store orders and checks the events by it.
   */
  @Override
  public long lastSequence(String aggregateId) {
    // reverseRange starts at the end of the aggregate's key range: the first key of this aggregate is its last event.
    try (KeyValueIterator<String, Event> keys = store.reverseRange(StoreKeys.first(aggregateId), StoreKeys.last(aggregateId))) {
      while (keys.hasNext()) {
        String key = keys.next().key;
        if (StoreKeys.isKeyOf(aggregateId, key)) {
          return StoreKeys.sequenceOf(aggregateId, key);
        }
        // another aggregate's key in the range, e.g. of "order-1@1": skipped
      }
      return 0;
    }
  }

  @Override
  public Events events(String aggregateId, long from, long to) {
    requireSequences(aggregateId, from, to);
    if (to < from) {
      return new OfAggregate(aggregateId, null); // an empty range: its start would come after its end
    }
    return new OfAggregate(aggregateId, store.range(StoreKeys.of(aggregateId, from), StoreKeys.of(aggregateId, to)));
  }

  @Override
  public Events eventsNewestFirst(String aggregateId, long from, long to) {
    requireSequences(aggregateId, to, from);
    if (from < to) {
      return new OfAggregate(aggregateId, null);
    }
    return new OfAggregate(aggregateId, store.reverseRange(StoreKeys.of(aggregateId, to), StoreKeys.of(aggregateId, from)));
  }

  private static void requireSequences(String aggregateId, long from, long to) {
    if (from < 1 || to < 1) {
      throw new IllegalArgumentException("Cannot read the events of aggregate '" + aggregateId + "' from sequence " + from + " to sequence " + to + ": a sequence starts at 1.");
    }
  }

  /** The events of one aggregate in a key range: the other aggregates' events in it are skipped. */
  private static class OfAggregate implements Events {

    private final String aggregateId;
    /** {@code null} for no events. */
    private final KeyValueIterator<String, Event> iterator;
    private Event next;

    OfAggregate(String aggregateId, KeyValueIterator<String, Event> iterator) {
      this.aggregateId = aggregateId;
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      while (next == null && iterator != null && iterator.hasNext()) {
        KeyValue<String, Event> entry = iterator.next();
        if (StoreKeys.isKeyOf(aggregateId, entry.key)) {
          next = entry.value;
        }
      }
      return next != null;
    }

    @Override
    public Event next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Event event = next;
      next = null;
      return event;
    }

    @Override
    public void close() {
      if (iterator != null) {
        iterator.close();
      }
    }
  }
}
