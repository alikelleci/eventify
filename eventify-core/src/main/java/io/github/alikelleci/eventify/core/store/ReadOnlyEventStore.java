package io.github.alikelleci.eventify.core.store;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.MessageIds;
import io.github.alikelleci.eventify.core.store.internal.EventStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Iterator;

/**
 * The stored events, read per aggregate. An aggregate's events are kept under their ids ({@link MessageIds}), in the
 * order they were handled: that is the order they are read in.
 *
 * <p>The key range of an aggregate can also hold another aggregate's events (see {@link MessageIds#isKeyOf}): they are
 * never returned.
 */
public interface ReadOnlyEventStore {

  /** Reads the events of a key-value store that holds them by id, e.g. a state store of Kafka Streams. */
  static ReadOnlyEventStore of(ReadOnlyKeyValueStore<String, Event> store) {
    return EventStore.readOnly(store);
  }

  /** The event with this id; {@code null} when there is none. */
  Event get(String eventId);

  /** All events of the aggregate, oldest first. */
  default Events events(String aggregateId) {
    return events(aggregateId, null, null);
  }

  /**
   * The events of the aggregate, oldest first.
   *
   * @param afterEventId the event to start after; {@code null} to start at the first event
   * @param untilEventId the last event, included; {@code null} to go on to the last event
   * @throws IllegalArgumentException when an event is not of this aggregate, or {@code untilEventId} comes before
   *                                  {@code afterEventId}
   */
  Events events(String aggregateId, String afterEventId, String untilEventId);

  /**
   * The events of the aggregate, newest first.
   *
   * @param untilEventId the first event returned, included; {@code null} to start at the newest event
   * @throws IllegalArgumentException when the event is not of this aggregate
   */
  Events eventsNewestFirst(String aggregateId, String untilEventId);

  /** Events read from the store. Close it once read, to release the store's iterator. */
  interface Events extends Iterator<Event>, AutoCloseable {
    @Override
    void close();
  }
}
