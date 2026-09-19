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
   */
  @Override
  public long lastSequence(String aggregateId) {
    try (Events events = eventsNewestFirst(aggregateId)) {
      return events.hasNext() ? events.next().getSequence() : 0;
    }
  }

  @Override
  public Events events(String aggregateId, long afterSequence, long untilSequence) {
    if (afterSequence < 0 || untilSequence < 0) {
      throw new IllegalArgumentException("Cannot read the events of aggregate '" + aggregateId + "' after sequence " + afterSequence + " up to sequence " + untilSequence + ": a sequence is never negative.");
    }
    if (untilSequence <= afterSequence) {
      return new OfAggregate(aggregateId, null); // nothing after it up to there; and not a range: its start would come after its end
    }
    return new OfAggregate(aggregateId, store.range(StoreKeys.of(aggregateId, afterSequence + 1), StoreKeys.of(aggregateId, untilSequence)));
  }

  @Override
  public Events eventsNewestFirst(String aggregateId, long untilSequence) {
    if (untilSequence < 1) {
      throw new IllegalArgumentException("Cannot read the events of aggregate '" + aggregateId + "' from sequence " + untilSequence + ": a sequence starts at 1.");
    }
    return new OfAggregate(aggregateId, store.reverseRange(StoreKeys.first(aggregateId), StoreKeys.of(aggregateId, untilSequence)));
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
