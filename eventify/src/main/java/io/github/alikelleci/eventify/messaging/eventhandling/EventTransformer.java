package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class EventTransformer implements ValueTransformerWithKey<String, Event, Event> {

  private final Eventify eventify;
  private ProcessorContext context;

  public EventTransformer(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
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
      Class<?> aggregateType = getAggregateType(eventSourcingHandler);
      saveEvent(event, aggregateType);
    }

    return event;
  }

  @Override
  public void close() {

  }

  private void saveEvent(Event event, Class<?> aggregateType) {
    getEventStore(aggregateType).putIfAbsent(event.getId(), ValueAndTimestamp.make(event, event.getTimestamp().toEpochMilli()));
  }

  private TimestampedKeyValueStore<String, Event> getEventStore(Class<?> aggregateType) {
    return context.getStateStore(aggregateType.getSimpleName() + "-event-store");
  }

  private TimestampedKeyValueStore<String, Aggregate> getSnapshotStore(Class<?> aggregateType) {
    return context.getStateStore(aggregateType.getSimpleName() + "-snapshot-store");
  }

  private Class<?> getAggregateType(EventSourcingHandler eventSourcingHandler) {
    return eventSourcingHandler.getMethod().getParameters()[0].getType();
  }
}
