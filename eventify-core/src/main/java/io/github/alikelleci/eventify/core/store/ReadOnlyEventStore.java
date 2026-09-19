package io.github.alikelleci.eventify.core.store;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.internal.EventStore;
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
    return EventStore.readOnly(store);
  }

  /** The aggregate's event with this sequence; {@code null} when there is none. */
  Event get(String aggregateId, long sequence);

  /** The sequence of the aggregate's last event: how many events it has had; 0 when it has none. */
  long lastSequence(String aggregateId);

  /** All events of the aggregate, oldest first. */
  default Events events(String aggregateId) {
    return events(aggregateId, 0, Long.MAX_VALUE);
  }

  /**
   * The events of the aggregate after a sequence, oldest first.
   *
   * @param afterSequence the sequence to start after, e.g. a snapshot's version; 0 to start at the first event
   */
  default Events events(String aggregateId, long afterSequence) {
    return events(aggregateId, afterSequence, Long.MAX_VALUE);
  }

  /**
   * The events of the aggregate between two sequences, oldest first.
   *
   * @param afterSequence the sequence to start after; 0 to start at the first event
   * @param untilSequence the last sequence, included; {@link Long#MAX_VALUE} to go on to the last event
   * @throws IllegalArgumentException when a sequence is negative
   */
  Events events(String aggregateId, long afterSequence, long untilSequence);

  /** All events of the aggregate, newest first. */
  default Events eventsNewestFirst(String aggregateId) {
    return eventsNewestFirst(aggregateId, Long.MAX_VALUE);
  }

  /**
   * The events of the aggregate, newest first.
   *
   * @param untilSequence the first sequence returned, included; {@link Long#MAX_VALUE} to start at the newest event
   * @throws IllegalArgumentException when the sequence is not positive
   */
  Events eventsNewestFirst(String aggregateId, long untilSequence);

  /** Events read from the store. Close it once read, to release the store's iterator. */
  interface Events extends Iterator<Event>, AutoCloseable {
    @Override
    void close();
  }
}
