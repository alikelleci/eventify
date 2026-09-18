package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.MessageIds;
import io.github.alikelleci.eventify.core.store.ReadOnlyEventStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The event store, stored by event id: {@code aggregateId@ULID}. The events of one aggregate are in one key range, in
 * the order they were handled.
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
  public Event get(String eventId) {
    return reads.get(eventId);
  }

  @Override
  public Events events(String aggregateId, String afterEventId, String untilEventId) {
    requireEventOf(aggregateId, afterEventId);
    requireEventOf(aggregateId, untilEventId);
    if (afterEventId != null && untilEventId != null) {
      int order = afterEventId.compareTo(untilEventId);
      if (order > 0) {
        throw new IllegalArgumentException("Cannot read the events of aggregate '" + aggregateId + "' after event '" + afterEventId + "' up to event '" + untilEventId + "': that event comes before it.");
      }
      if (order == 0) {
        return new OfAggregate(aggregateId, null); // nothing after it up to itself; and not a range: its start would come after its end
      }
    }
    String from = afterEventId != null ? afterEventId + "\0" : MessageIds.firstKey(aggregateId); // after that event
    String to = untilEventId != null ? untilEventId : MessageIds.lastKey(aggregateId);
    return new OfAggregate(aggregateId, reads.range(from, to));
  }

  @Override
  public Events eventsNewestFirst(String aggregateId, String untilEventId) {
    requireEventOf(aggregateId, untilEventId);
    String to = untilEventId != null ? untilEventId : MessageIds.lastKey(aggregateId);
    return new OfAggregate(aggregateId, reads.reverseRange(MessageIds.firstKey(aggregateId), to));
  }

  /** A range bounded by another aggregate's event would cover the events stored in between. */
  private static void requireEventOf(String aggregateId, String eventId) {
    if (eventId != null && !MessageIds.isKeyOf(aggregateId, eventId)) {
      throw new IllegalArgumentException("Event '" + eventId + "' is not an event of aggregate '" + aggregateId + "'.");
    }
  }

  /**
   * The id of the aggregate's last stored event; {@code null} when it has none. Deleting events at a snapshot keeps
   * the snapshot's event and the ones after it, so the last event is never deleted.
   */
  public String lastEventId(String aggregateId) {
    try (Events events = eventsNewestFirst(aggregateId, null)) {
      return events.hasNext() ? events.next().getId() : null;
    }
  }

  /**
   * The events under ids after the aggregate's last stored event, in the order they are given. The store order is the
   * replay order: it must be the order the events were handled in, not the order of the commands' timestamps or of the
   * clocks of the hosts that handled them.
   */
  public List<Event> assignIds(String aggregateId, List<Event> events) {
    String lastId = lastEventId(aggregateId);
    List<Event> ordered = new ArrayList<>(events.size());
    for (Event event : events) {
      Event withId = event.withId(MessageIds.nextEventKey(aggregateId, lastId));
      ordered.add(withId);
      lastId = withId.getId();
    }
    return ordered;
  }

  public void append(Event event) {
    writes.putIfAbsent(event.getId(), event);
  }

  /**
   * Deletes the aggregate's events before the snapshot's event; the snapshot's event itself is kept.
   *
   * @return how many events were deleted
   */
  public long deleteBefore(AggregateState snapshot) {
    long deleted = 0;
    String aggregateId = snapshot.getAggregateId();
    try (KeyValueIterator<String, Event> iterator = writes.range(MessageIds.firstKey(aggregateId), snapshot.getEventId())) {
      while (iterator.hasNext()) {
        KeyValue<String, Event> entry = iterator.next();
        if (entry.key.equals(snapshot.getEventId())) {
          break; // keep the snapshot event itself
        }
        if (!MessageIds.isKeyOf(aggregateId, entry.key)) {
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
        if (MessageIds.isKeyOf(aggregateId, entry.key)) {
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
