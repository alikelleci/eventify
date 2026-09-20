package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.util.RawValue;
import io.github.alikelleci.eventify.console.client.ConsoleViews.CommandEventsPage;
import io.github.alikelleci.eventify.console.client.ConsoleViews.CommandsPage;
import io.github.alikelleci.eventify.console.client.ConsoleViews.EventDetail;
import io.github.alikelleci.eventify.console.client.ConsoleViews.EventsPage;
import io.github.alikelleci.eventify.console.client.ConsoleViews.Result;
import io.github.alikelleci.eventify.console.protocol.NodeStatus;
import io.github.alikelleci.eventify.console.protocol.Requests;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.exception.EventReplayException;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.plugin.PluginContext;
import io.github.alikelleci.eventify.core.store.EventStore;
import io.github.alikelleci.eventify.core.store.SnapshotStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;

/**
 * Answers the console's queries about this node and the aggregates it owns. Reading and retrying commands is done by
 * {@link CommandHistory} and {@link CommandRetry}.
 */
@Slf4j
class ConsoleService {

  private final PluginContext eventify;
  private final StatusTracker statusTracker;
  private final Ownership ownership;
  private final AggregateHistory history;
  private final CommandHistory commandHistory;
  private final CommandRetry commandRetry;

  ConsoleService(PluginContext eventify, StatusTracker statusTracker) {
    this.eventify = eventify;
    this.statusTracker = statusTracker;
    this.ownership = new Ownership(eventify);
    this.history = new AggregateHistory(eventify.getAggregateReplayer(), eventify.getObjectMapper());
    this.commandHistory = new CommandHistory(eventify);
    this.commandRetry = new CommandRetry(eventify);
  }

  /**
   * How this instance is doing: its Kafka Streams state, how long it has been in it, and whether it is restoring.
   * Everything is read from what Kafka Streams already keeps in memory: no calls to Kafka, and not on the stream threads.
   */
  Result<NodeStatus> getStatus() {
    KafkaStreams streams = eventify.getKafkaStreams();
    if (streams == null) {
      return Result.unavailable("Eventify is not started");
    }

    return Result.ok(new NodeStatus(streams.state().name(), statusTracker.stateForMs(), statusTracker.restoring()));
  }

  void close() {
    commandRetry.close();
  }

  /** @see CommandRetry#retry */
  Result<Void> retryCommand(byte[] json) {
    return commandRetry.retry(json);
  }

  /** @see CommandHistory#read */
  Result<CommandsPage> getCommands(String aggregateId, int limit, CancelSignal cancel) {
    return commandHistory.read(aggregateId, limit, cancel);
  }

  Result<CommandEventsPage> getEventsOfCommand(Requests.EventsOfCommand request) {
    String aggregateId = request.aggregateId();
    Result<CommandEventsPage> routing = ownership.check(aggregateId);
    if (routing != null) return routing;

    try {
      return Result.ok(new CommandEventsPage(history.eventsOfCommand(eventStore(), request)));
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return Result.unavailable("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying the events of command {} of aggregate {}", request.commandId(), aggregateId, e);
      return Result.unavailable("Unexpected error");
    }
  }

  Result<EventsPage> getEvents(String aggregateId, Long cursor, int limit) {
    Result<EventsPage> routing = ownership.check(aggregateId);
    if (routing != null) {
      return routing;
    }

    try {
      return Result.ok(history.events(eventStore(), aggregateId, cursor, limit));
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return Result.unavailable("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying events for aggregate {}", aggregateId, e);
      return Result.unavailable("Unexpected error");
    }
  }

  Result<EventDetail> getEventDetail(String aggregateId, long sequence) {
    Result<EventDetail> routing = ownership.check(aggregateId);
    if (routing != null) {
      return routing;
    }

    try {
      EventDetail detail = history.eventDetail(eventStore(), snapshotStore(), aggregateId, sequence);
      if (detail == null) {
        return ownership.notFound(aggregateId);
      }
      return Result.ok(detail);
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return Result.unavailable("Event store not ready");
    } catch (EventReplayException e) {
      // The stored events can't be replayed: the application refuses the aggregate's commands for the same reason.
      log.warn("Cannot replay aggregate {}: {}", aggregateId, e.getMessage());
      return Result.unreadable(e.getMessage());
    } catch (Exception e) {
      log.error("Unexpected error querying event detail for aggregate {}", aggregateId, e);
      return Result.unavailable("Unexpected error");
    }
  }

  /** The {@link AggregateState} as JSON, see {@link AggregateHistory}. */
  Result<RawValue> getState(String aggregateId, Long sequence) {
    Result<RawValue> routing = ownership.check(aggregateId);
    if (routing != null) {
      return routing;
    }

    try {
      RawValue state = history.stateAt(eventStore(), snapshotStore(), aggregateId, sequence);
      if (state == null) {
        return ownership.notFound(aggregateId);
      }
      return Result.ok(state);
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return Result.unavailable("Event store not ready");
    } catch (EventReplayException e) {
      // The stored events can't be replayed: the application refuses the aggregate's commands for the same reason.
      log.warn("Cannot replay aggregate {}: {}", aggregateId, e.getMessage());
      return Result.unreadable(e.getMessage());
    } catch (Exception e) {
      log.error("Unexpected error querying state for aggregate {}", aggregateId, e);
      return Result.unavailable("Unexpected error");
    }
  }

  private EventStore eventStore() {
    return eventify.getEventStore();
  }

  private SnapshotStore snapshotStore() {
    return eventify.getSnapshotStore();
  }

}
