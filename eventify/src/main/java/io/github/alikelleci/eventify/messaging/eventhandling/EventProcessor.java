package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.eventsourcing.AggregateState;
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
  private KeyValueStore<String, AggregateState> snapshotStore;

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
      AggregateState state = loadSnapshot(key);

      // TODO: fix this, a delete can trigger this twice
      if (state == null || !StringUtils.equals(state.getEventId(), event.getId())) {
        // 2. Apply event
        state = eventSourcingHandler.apply(state, event);

        // 3. Save snapshot
        if (state != null) {
          log.debug("Creating snapshot: {}", state);
          saveSnapshot(state);
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

  protected AggregateState loadSnapshot(String aggregateId) {
    return snapshotStore.get(aggregateId);
  }

  protected void saveSnapshot(AggregateState state) {
    snapshotStore.put(state.getAggregateId(), state);
  }

  private void deleteSnapshot(String key) {
    snapshotStore.delete(key);
  }
}
