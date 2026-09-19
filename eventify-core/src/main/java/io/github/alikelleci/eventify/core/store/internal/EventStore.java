package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.StoreKeys;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

/** The event store as command handling uses it: read (see {@link EventStoreReader}) and written. */
public class EventStore extends EventStoreReader {

  private final KeyValueStore<String, Event> store;

  public EventStore(KeyValueStore<String, Event> store) {
    super(store);
    this.store = store;
  }

  /**
   * The events with the sequences after the aggregate's last stored event, in the order they are given. The sequence
   * is the replay order: the order the events were handled in, whatever the commands' timestamps or the clocks of the
   * hosts that handled them.
   */
  public List<Event> sequence(String aggregateId, List<Event> events) {
    long last = lastSequence(aggregateId);
    List<Event> sequenced = new ArrayList<>(events.size());
    for (Event event : events) {
      sequenced.add(event.withSequence(++last));
    }
    return sequenced;
  }

  /**
   * Stores the event under its sequence.
   *
   * @throws IllegalStateException when the event has no sequence, or its sequence is taken: the aggregate's events would
   *                               no longer be the ones that were handled
   */
  public void append(Event event) {
    if (event.getSequence() < 1) {
      throw new IllegalStateException("Event " + event.getId() + " of aggregate " + event.getAggregateId() + " has no sequence: it can't be stored.");
    }
    Event taken = store.putIfAbsent(StoreKeys.of(event.getAggregateId(), event.getSequence()), event);
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
    try (KeyValueIterator<String, Event> iterator = store.range(StoreKeys.first(aggregateId), StoreKeys.of(aggregateId, snapshot.getVersion() - 1))) {
      while (iterator.hasNext()) {
        KeyValue<String, Event> entry = iterator.next();
        if (!StoreKeys.isKeyOf(aggregateId, entry.key)) {
          continue; // another aggregate's event in the range: never ours to delete
        }
        store.delete(entry.key);
        deleted++;
      }
    }
    return deleted;
  }
}
