package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class Repository {

  private final TimestampedKeyValueStore<String, Event> eventStore;
  private final TimestampedKeyValueStore<String, Aggregate> snapshotStore;
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;

  public Repository(ProcessorContext context, Map<Class<?>, EventSourcingHandler> eventSourcingHandlers) {
    this.eventSourcingHandlers = eventSourcingHandlers;
    this.eventStore = context.getStateStore("event-store");
    this.snapshotStore = context.getStateStore("snapshot-store");
  }

  public Aggregate loadAggregate(String aggregateId) {
    AtomicLong sequence = new AtomicLong(0);
    AtomicLong counter = new AtomicLong(0);

    String from = aggregateId.concat("@");
    String to = aggregateId.concat("@z");

    Aggregate aggregate = loadFromSnapshot(aggregateId);
    if (aggregate != null) {
      log.debug("Snapshot found: {}", aggregate);
      from = aggregate.getEventId();
      sequence.set(aggregate.getVersion());
    }

    log.debug("Loading aggregate state by applying events...");

    try (KeyValueIterator<String, ValueAndTimestamp<Event>> iterator = eventStore.range(from, to)) {
      while (iterator.hasNext()) {
        Event event = iterator.next().value.value();
        if (aggregate == null || !aggregate.getEventId().equals(event.getId())) {
          EventSourcingHandler eventSourcingHandler = eventSourcingHandlers.get(event.getPayload().getClass());
          if (eventSourcingHandler != null) {
            aggregate = eventSourcingHandler.apply(event, aggregate);

            sequence.incrementAndGet();
            counter.incrementAndGet();
          }
        }
      }
    }

    aggregate = Optional.ofNullable(aggregate)
        .map(aggr -> Aggregate.builder()
            .timestamp(aggr.getTimestamp())
            .payload(aggr.getPayload())
            .metadata(aggr.getMetadata())
            .aggregateId(aggr.getAggregateId())
            .eventId(aggr.getEventId())
            .version(sequence.get())
            .build())
        .orElse(null);

    log.debug("Total events applied: {}", counter.get());
    log.debug("Current aggregate state reconstructed: {}", aggregate);
    return aggregate;
  }

  public void saveEvent(Event event) {
    eventStore.putIfAbsent(event.getId(), ValueAndTimestamp.make(event, event.getTimestamp().toEpochMilli()));
  }

  public void saveSnapshot(Aggregate aggregate) {
    snapshotStore.put(aggregate.getAggregateId(), ValueAndTimestamp.make(aggregate, aggregate.getTimestamp().toEpochMilli()));
  }

  public void deleteEvents(Aggregate aggregate) {
    AtomicLong counter = new AtomicLong(0);

    String from = aggregate.getAggregateId().concat("@");
    String to = aggregate.getEventId();

    try (KeyValueIterator<String, ValueAndTimestamp<Event>> iterator = eventStore.range(from, to)) {
      while (iterator.hasNext()) {
        Event event = iterator.next().value.value();

        log.debug("Deleting event: {} ({})", event.getPayload().getClass().getSimpleName(), event.getAggregateId());
        eventStore.delete(event.getId());

        counter.incrementAndGet();
      }
    }
    log.debug("Total events deleted: {}", counter.get());
  }

  private Aggregate loadFromSnapshot(String aggregateId) {
    return Optional.ofNullable(snapshotStore.get(aggregateId))
        .map(ValueAndTimestamp::value)
        .orElse(null);
  }


}
