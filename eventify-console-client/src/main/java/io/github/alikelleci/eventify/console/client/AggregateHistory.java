package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.RawValue;
import io.github.alikelleci.eventify.core.aggregate.AggregateRepository;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.event.Event;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregate history queries through the repository's public read API. Only the latest snapshot is retained; when
 * older events were deleted, states before that snapshot cannot be reconstructed. A known absent payload is exposed
 * as null to the console, while the repository keeps its version in an AggregateState.
 *
 * <p>States are captured as JSON immediately: event sourcing handlers may mutate the same payload on the next event.
 */
class AggregateHistory {

  private final ObjectMapper objectMapper;

  AggregateHistory(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /** Newest first; cursor is inclusive. Read one extra event to determine the next page's cursor. */
  ConsoleViews.EventsPage events(AggregateRepository repository, String aggregateId, Long cursor, int limit) {
    List<Event> page = new ArrayList<>();
    try (var newestFirst = repository.eventsNewestFirst(aggregateId, cursor != null ? cursor : Long.MAX_VALUE, 1)) {
      while (newestFirst.hasNext() && page.size() <= limit) {
        page.add(newestFirst.next());
      }
    }
    Long nextCursor = page.size() > limit ? page.remove(page.size() - 1).getSequence() : null;
    return new ConsoleViews.EventsPage(page, nextCursor);
  }

  /** Match causation, not correlation: commands in one saga can share a correlation id. */
  List<Event> eventsOfCommand(AggregateRepository repository, String aggregateId, String commandId) {
    List<Event> produced = new ArrayList<>();
    try (var events = repository.events(aggregateId)) {
      while (events.hasNext()) {
        Event event = events.next();
        if (commandId.equals(event.getMetadata().getCausationId())) {
          produced.add(event);
        }
      }
    }
    return produced;
  }

  RawValue stateAt(AggregateRepository repository, String aggregateId, Long sequence) {
    if (sequence != null && repository.event(aggregateId, sequence) == null) {
      return null;
    }
    return json(repository.replay(aggregateId, sequence != null ? sequence : Long.MAX_VALUE, null));
  }

  ConsoleViews.EventDetail eventDetail(AggregateRepository repository, String aggregateId, long sequence) {
    Event event = repository.event(aggregateId, sequence);
    if (event == null) {
      return null;
    }
    RawValue[] previous = {null};
    boolean[] sawEvent = {false};
    AggregateState state = repository.replay(aggregateId, sequence, (current, before) -> {
      if (current.getSequence() == sequence) {
        sawEvent[0] = true;
        previous[0] = json(before);
      }
    });
    if (state == null) {
      return ConsoleViews.EventDetail.withUnknownStates(event);
    }
    if (!sawEvent[0]) {
      return ConsoleViews.EventDetail.withUnknownPreviousState(event, json(state));
    }
    return ConsoleViews.EventDetail.known(event, json(state), previous[0]);
  }

  private RawValue json(AggregateState state) {
    if (state == null || state.getPayload() == null) {
      return null;
    }
    try {
      return new RawValue(objectMapper.writeValueAsString(state));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot write the state of " + state.getType() + " " + state.getAggregateId() + " as JSON", e);
    }
  }
}
