package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.ReadOnlyEventStore;
import io.github.alikelleci.eventify.core.store.StoreKeys;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The event store, stored by {@link StoreKeys}: {@code aggregateId@sequence}. The events of one aggregate are in one
 * key range, in the order of their sequence: the order they were handled in.
 */
public class EventStore implements ReadOnlyEventStore {

  private final ReadOnlyKeyValueStore<String, Event> reads;
  /** {@code null} when read-only. */
  private final KeyValueStore<String, Event> writes;

  public EventStore(KeyValueStore<String, Event> store) {
    this(store, store);
  }

  private EventStore(ReadOnlyKeyValueStore<String, Event> reads, KeyValueStore<String, Event> writes) {
    this.reads = reads;
    this.writes = writes;
  }

  public static ReadOnlyEventStore readOnly(ReadOnlyKeyValueStore<String, Event> store) {
    return new EventStore(store, null);
  }

  @Override
  public Event get(String aggregateId, long sequence) {
    return sequence < 1 ? null : reads.get(StoreKeys.of(aggregateId, sequence));
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
    return new OfAggregate(aggregateId, reads.range(StoreKeys.of(aggregateId, afterSequence + 1), StoreKeys.of(aggregateId, untilSequence)));
  }

  @Override
  public Events eventsNewestFirst(String aggregateId, long untilSequence) {
    if (untilSequence < 1) {
      throw new IllegalArgumentException("Cannot read the events of aggregate '" + aggregateId + "' from sequence " + untilSequence + ": a sequence starts at 1.");
    }
    return new OfAggregate(aggregateId, reads.reverseRange(StoreKeys.first(aggregateId), StoreKeys.of(aggregateId, untilSequence)));
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
    Event taken = writes.putIfAbsent(StoreKeys.of(event.getAggregateId(), event.getSequence()), event);
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
    try (KeyValueIterator<String, Event> iterator = writes.range(StoreKeys.first(aggregateId), StoreKeys.of(aggregateId, snapshot.getVersion() - 1))) {
      while (iterator.hasNext()) {
        KeyValue<String, Event> entry = iterator.next();
        if (!StoreKeys.isKeyOf(aggregateId, entry.key)) {
          continue; // another aggregate's event in the range: never ours to delete
        }
        writes.delete(entry.key);
        deleted++;
      }
    }
    return deleted;
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
