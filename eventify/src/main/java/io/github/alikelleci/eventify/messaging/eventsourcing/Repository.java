package io.github.alikelleci.eventify.messaging.eventsourcing;

import io.github.alikelleci.eventify.constants.Handlers;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class Repository {

  private final TimestampedKeyValueStore<String, Event> eventStore;
  private final TimestampedKeyValueStore<String, Aggregate> snapshotStore;

  public Repository(ProcessorContext context) {
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
          EventSourcingHandler eventSourcingHandler = Handlers.EVENTSOURCING_HANDLERS.get(event.getPayload().getClass());
          if (eventSourcingHandler != null) {
            aggregate = eventSourcingHandler.apply(aggregate, event);

            sequence.incrementAndGet();
            counter.incrementAndGet();
          }
        }
      }
    }

    aggregate = Optional.ofNullable(aggregate)
        .map(aggr -> aggr.toBuilder()
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

  private Aggregate loadFromSnapshot(String aggregateId) {
    return Optional.ofNullable(snapshotStore.get(aggregateId))
        .map(ValueAndTimestamp::value)
        .orElse(null);
  }


}
