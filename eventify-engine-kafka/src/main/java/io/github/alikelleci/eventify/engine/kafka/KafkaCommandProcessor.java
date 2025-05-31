package io.github.alikelleci.eventify.engine.kafka;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandProcessor;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.support.AutoCloseableIterator;
import io.github.alikelleci.eventify.engine.kafka.support.EventIterator;
import jakarta.validation.ValidationException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class KafkaCommandProcessor extends CommandProcessor implements FixedKeyProcessor<String, Command, CommandResult> {

  private final Eventify eventify;
  private FixedKeyProcessorContext<String, CommandResult> context;
  private KeyValueStore<String, Event> eventStore;
  private KeyValueStore<String, AggregateState> snapshotStore;

  public KafkaCommandProcessor(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, CommandResult> context) {
    this.context = context;
    this.eventStore = context.getStateStore("event-store");
    this.snapshotStore = context.getStateStore("snapshot-store");
  }

  @Override
  public void process(FixedKeyRecord<String, Command> fixedKeyRecord) {
    String key = fixedKeyRecord.key();
    Command command = fixedKeyRecord.value();

    try {
      // Execute command
      List<Event> events = executeCommand(key, command);

      // Return if no events
      if (CollectionUtils.isEmpty(events)) {
        return;
      }

      // Forward success
      context.forward(fixedKeyRecord.withValue(Success.builder()
          .command(command)
          .events(events)
          .build()));

    } catch (Exception e) {
      // Log failure
      logFailure(e);

      // Forward failure
      context.forward(fixedKeyRecord.withValue(Failure.builder()
          .command(command)
          .cause(ExceptionUtils.getRootCauseMessage(e))
          .build()));
    }
  }

  @Override
  protected AggregateState loadFromSnapshot(String aggregateId) {
    return snapshotStore.get(aggregateId);
  }

  @Override
  protected AutoCloseableIterator<Event> loadEvents(String aggregateId, String from, String to) {
    return new EventIterator(eventStore.range(from, to));
  }

  @Override
  protected void saveEvent(Event event) {
    eventStore.putIfAbsent(event.getId(), event);
  }

  @Override
  protected Eventify getEventify() {
    return eventify;
  }

  @Override
  protected void saveSnapshot(AggregateState state) {
    snapshotStore.put(state.getAggregateId(), state);
  }

  @Override
  protected void deleteEvents(Iterator<Event> events) {
    AtomicInteger counter = new AtomicInteger();

    while (events.hasNext()) {
      Event event = events.next();
      eventStore.delete(event.getId());

      counter.incrementAndGet();
    }

    log.debug("Number of events deleted: {}", counter);
  }

  @Override
  public void close() {

  }

  private void logFailure(Exception e) {
    Throwable throwable = ExceptionUtils.getRootCause(e);
    String message = ExceptionUtils.getRootCauseMessage(e);

    if (throwable instanceof ValidationException) {
      log.debug("Handling command failed: {}", message, throwable);
    } else {
      log.error("Handling command failed: {}", message, throwable);
    }
  }
}
