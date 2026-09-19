package io.github.alikelleci.eventify.core.store;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.internal.EventStoreReader;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Iterator;

/**
 * The stored events, read per aggregate. An aggregate's events are kept by their sequence ({@link StoreKeys}): 1 for
 * its first event, then one more for each next one. That is the order they were handled in, and the order they are
 * read in.
 */
public interface ReadOnlyEventStore {

  /** Reads the events of a key-value store that holds them by {@link StoreKeys}, e.g. a state store of Kafka Streams. */
  static ReadOnlyEventStore of(ReadOnlyKeyValueStore<String, Event> store) {
    return new EventStoreReader(store);
  }

  /** The aggregate's event with this sequence; {@code null} when there is none. */
  Event get(String aggregateId, long sequence);

  /** The sequence of the aggregate's last event: how many events it has had; 0 when it has none. */
  long lastSequence(String aggregateId);

  /** All events of the aggregate, oldest first. */
  default Events events(String aggregateId) {
    return events(aggregateId, 1, Long.MAX_VALUE);
  }

  /**
   * The events of the aggregate from one sequence to another, oldest first. Both are included: to read on after a
   * snapshot at version 40, start at 41.
   *
   * @param from where to start, e.g. 1 for the first event
   * @param to   where to stop, e.g. {@link Long#MAX_VALUE} for the last event; nothing is read when it is below
   *             {@code from}
   * @throws IllegalArgumentException when a sequence is below 1
   */
  Events events(String aggregateId, long from, long to);

  /** All events of the aggregate, newest first. */
  default Events eventsNewestFirst(String aggregateId) {
    return eventsNewestFirst(aggregateId, Long.MAX_VALUE, 1);
  }

  /**
   * The events of the aggregate from one sequence back to another, newest first. Both are included.
   *
   * @param from where to start, e.g. {@link Long#MAX_VALUE} for the last event
   * @param to   where to stop, e.g. 1 for the first event; nothing is read when it is above {@code from}
   * @throws IllegalArgumentException when a sequence is below 1
   */
  Events eventsNewestFirst(String aggregateId, long from, long to);

  /** Events read from the store. Close it once read, to release the store's iterator. */
  interface Events extends Iterator<Event>, AutoCloseable {
    @Override
    void close();
  }
}
