package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.StoreKeys;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


/** The event store as command handling uses it: read (see {@link ReadableEventStore}) and written. */
public class WritableEventStore extends ReadableEventStore {

  private final KeyValueStore<String, Event> store;
  private final String aggregateType;

  public WritableEventStore(KeyValueStore<String, Event> store, String aggregateType) {
    super(store, aggregateType);
    this.store = store;
    this.aggregateType = aggregateType;
  }

  /**
   * Stores the event under its sequence.
   *
   * @throws IllegalStateException when its sequence is taken: the aggregate's events would no longer be the ones that
   *                               were handled
   */
  public void append(Event event) {
    Event taken = store.putIfAbsent(StoreKeys.of(aggregateType, event.getAggregateId(), event.getSequence()), event);
    if (taken != null) {
      throw new IllegalStateException("Aggregate " + event.getAggregateId() + " already has an event with sequence " + event.getSequence() + ": event " + taken.getId() + ".");
    }
  }

  /**
   * Deletes the aggregate's events before the snapshot's event; the snapshot's event itself is kept.
   *
   * @return how many events were deleted
   */
  public long deleteBefore(AggregateState snapshot) {
    if (snapshot.getVersion() <= 1) {
      return 0;
    }
    long deleted = 0;
    String aggregateId = snapshot.getAggregateId();
    try (KeyValueIterator<String, Event> iterator = store.range(StoreKeys.first(aggregateType, aggregateId), StoreKeys.of(aggregateType, aggregateId, snapshot.getVersion() - 1))) {
      while (iterator.hasNext()) {
        store.delete(iterator.next().key);
        deleted++;
      }
    }
    return deleted;
  }
}
