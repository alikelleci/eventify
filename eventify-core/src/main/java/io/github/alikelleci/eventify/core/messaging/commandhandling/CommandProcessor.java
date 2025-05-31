package io.github.alikelleci.eventify.core.messaging.commandhandling;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.core.support.AutoCloseableIterator;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class CommandProcessor {

  protected abstract Eventify getEventify();
  protected abstract AggregateState loadFromSnapshot(String aggregateId);
  protected abstract void saveSnapshot(AggregateState state);
  protected abstract AutoCloseableIterator<Event> loadEvents(String aggregateId, String from, String to);
  protected abstract void deleteEvents(Iterator<Event> events);
  protected abstract void saveEvent(Event event);

  protected List<Event> executeCommand(String aggregateId, Command command) {
    CommandHandler commandHandler = getEventify().getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      log.debug("No Command Handler found for command: {} ({})", command.getType(), command.getAggregateId());
      return new ArrayList<>();
    }

    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());
    AggregateState state = loadAggregate(aggregateId);
    List<Event> events = commandHandler.apply(state, command);

    for (Event event : events) {
      saveEvent(event);
    }

    return events;
  }

  private AggregateState loadAggregate(String aggregateId) {
    Instant startTime = Instant.now();

    AtomicLong sequence = new AtomicLong(0);
    AtomicLong counter = new AtomicLong(0);

    String from = aggregateId.concat("@");
    String to = aggregateId.concat("@~");

    AggregateState state = loadFromSnapshot(aggregateId);
    if (state != null) {
      log.debug("Snapshot found: {}", state);

      from = state.getEventId();
      sequence.set(state.getVersion());
    }

    log.debug("Loading aggregate state by applying events...");

    try (AutoCloseableIterator<Event> iterator = loadEvents(aggregateId, from, to)) {
      while (iterator.hasNext()) {
        Event event = iterator.next();
        if (state == null || !state.getEventId().equals(event.getId())) {
          EventSourcingHandler eventSourcingHandler = getEventify().getEventSourcingHandlers().get(event.getPayload().getClass());
          if (eventSourcingHandler != null) {
            log.trace("Applying event: {} ({})", event.getType(), event.getAggregateId());
            state = eventSourcingHandler.apply(state, event);

            sequence.incrementAndGet();
            counter.incrementAndGet();
          }
        }
      }
    }

    state = Optional.ofNullable(state)
        .map(aggr -> AggregateState.builder()
            .timestamp(aggr.getTimestamp())
            .payload(aggr.getPayload())
            .metadata(aggr.getMetadata())
            .eventId(aggr.getEventId())
            .version(sequence.get())
            .build())
        .orElse(null);

    Instant endTime = Instant.now();
    Duration duration = Duration.between(startTime, endTime);

    log.debug("Number of events applied: {}", counter.get());
    log.debug("Aggregate state reconstructed in {} ms ({} sec): {}", duration.toMillis(), duration.toSeconds(), state);

    // Save snapshot if needed
    Optional.ofNullable(state)
        .filter(s -> counter.get() > 0)
        .filter(s -> s.getSnapshotThreshold() > 0)
        .filter(s -> s.getVersion() % s.getSnapshotThreshold() == 0)
        .ifPresent(s -> {
          log.debug("Creating snapshot: {}", s);
          saveSnapshot(s);

          // Delete events after snapshot
          if (s.deleteEvents()) {
            log.debug("Events prior to this snapshot will be deleted");
            deleteEvents(s);
          }
        });

    return state;
  }

  private void deleteEvents(AggregateState state) {
    String from = state.getAggregateId().concat("@");
    String to = state.getEventId();

    try (AutoCloseableIterator<Event> foundEvents = loadEvents(state.getAggregateId(), from, to)) {
      deleteEvents(foundEvents);
    }
  }
}
