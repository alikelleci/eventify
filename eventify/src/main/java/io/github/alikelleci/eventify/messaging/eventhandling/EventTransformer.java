package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;

@Slf4j
public class EventTransformer implements ValueTransformerWithKey<String, Event, Event> {

  private final Eventify eventify;
  private TimestampedKeyValueStore<String, Aggregate> snapshotStore;

  public EventTransformer(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.snapshotStore = processorContext.getStateStore("snapshot-store");
  }

  @Override
  public Event transform(String key, Event event) {
    Collection<EventHandler> eventHandlers = eventify.getEventHandlers().get(event.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(eventHandlers)) {
      eventHandlers.stream()
          .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
          .forEach(handler ->
              handler.apply(event));
    }

    EventSourcingHandler eventSourcingHandler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
    if (eventSourcingHandler != null) {
      // 1. Load aggregate state
      Aggregate aggregate = loadSnapshot(key);

      if (aggregate == null || !StringUtils.equals(aggregate.getEventId(), event.getId())) {
        // 2. Apply event
        aggregate = eventSourcingHandler.apply(event, aggregate);

        // 3. Save snapshot
        Optional.ofNullable(aggregate)
            .ifPresent(this::saveSnapshot);

        if (aggregate == null) {
          deleteSnapshot(key);
        }
      }
    }

    return event;
  }

  @Override
  public void close() {

  }

  protected Aggregate loadSnapshot(String aggregateId) {
    return Optional.ofNullable(snapshotStore.get(aggregateId))
        .map(ValueAndTimestamp::value)
        .orElse(null);
  }

  protected void saveSnapshot(Aggregate aggregate) {
    snapshotStore.put(aggregate.getAggregateId(), ValueAndTimestamp.make(aggregate, aggregate.getTimestamp().toEpochMilli()));
  }

  private void deleteSnapshot(String key) {
    snapshotStore.delete(key);
  }
}
