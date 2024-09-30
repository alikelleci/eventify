package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class EventProcessor implements FixedKeyProcessor<String, Event, Event> {

  private final Eventify eventify;
  private FixedKeyProcessorContext<String, Event> context;
  private KeyValueStore<String, Aggregate> snapshotStore;

  public EventProcessor(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, Event> context) {
    this.context = context;
    this.snapshotStore = context.getStateStore("snapshot-store");
  }

  @Override
  public void process(FixedKeyRecord<String, Event> fixedKeyRecord) {
    String key = fixedKeyRecord.key();
    Event event = fixedKeyRecord.value();

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

      // TODO: fix this, a delete can trigger this twice
      if (aggregate == null || !StringUtils.equals(aggregate.getEventId(), event.getId())) {
        // 2. Apply event
        aggregate = eventSourcingHandler.apply(aggregate, event);

        // 3. Save snapshot
        if (aggregate != null) {
          log.debug("Creating snapshot: {}", aggregate);
          saveSnapshot(aggregate);
        } else {
          deleteSnapshot(key);
        }
      }
    }

    context.forward(fixedKeyRecord);
  }

  @Override
  public void close() {

  }

  protected Aggregate loadSnapshot(String aggregateId) {
    return snapshotStore.get(aggregateId);
  }

  protected void saveSnapshot(Aggregate aggregate) {
    snapshotStore.put(aggregate.getAggregateId(), aggregate);
  }

  private void deleteSnapshot(String key) {
    snapshotStore.delete(key);
  }
}
