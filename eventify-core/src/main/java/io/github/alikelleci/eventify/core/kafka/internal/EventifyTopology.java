package io.github.alikelleci.eventify.core.kafka.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.internal.CommandProcessor;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.event.internal.EventProcessor;
import io.github.alikelleci.eventify.core.handler.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.kafka.TopicNames;
import io.github.alikelleci.eventify.core.serialization.JsonSerde;
import io.github.alikelleci.eventify.core.store.internal.SnapshotSerde;
import io.github.alikelleci.eventify.core.store.internal.StoreNames;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.REPLY_TO;

/**
 * The Kafka Streams topology of an Eventify application: the event and snapshot stores, command handling (results,
 * replies and events out) and event handling.
 */
public final class EventifyTopology {

  private EventifyTopology() {
  }

  public static Topology build(HandlerRegistry handlers, ObjectMapper objectMapper) {
    StreamsBuilder builder = new StreamsBuilder();

    /*
     * -------------------------------------------------------------
     * SERDES
     * -------------------------------------------------------------
     */

    Serde<Command> commandSerde = new CommandSerde(objectMapper);
    Serde<CommandResult> resultSerde = new JsonSerde<>(CommandResult.class, objectMapper);
    Serde<Event> eventSerde = new EventSerde(objectMapper, handlers.upcasters());
    Serde<AggregateState> snapshotSerde = new SnapshotSerde(objectMapper);

    /*
     * -------------------------------------------------------------
     * STORES
     * -------------------------------------------------------------
     */

    // Event store
    builder.addStateStore(Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(StoreNames.EVENT_STORE), Serdes.String(), eventSerde)
        .withLoggingEnabled(Collections.emptyMap()));

    // Snapshot Store
    builder.addStateStore(Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(StoreNames.SNAPSHOT_STORE), Serdes.String(), snapshotSerde)
        .withLoggingEnabled(Collections.emptyMap()));

    /*
     * -------------------------------------------------------------
     * COMMAND HANDLING
     * -------------------------------------------------------------
     */

    Set<String> commandTopics = handlers.commandTopics();
    if (!commandTopics.isEmpty()) {
      // --> Commands
      KStream<String, Command> commands = builder.stream(commandTopics, Consumed.with(Serdes.String(), commandSerde))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null)
          .filter((key, command) -> command.getPayload() != null);

      // Commands --> Results
      KStream<String, CommandResult> commandResults = commands
          .processValues(() -> new CommandProcessor(handlers, objectMapper), StoreNames.EVENT_STORE, StoreNames.SNAPSHOT_STORE)
          .filter((key, result) -> result != null);

      // Results --> Push
      commandResults
          .to((key, result, recordContext) -> TopicNames.resultTopicOf(result.command().getTopic().value()),
              Produced.with(Serdes.String(), resultSerde));

      // Results --> Push to reply topic
      commandResults
          .filter((key, result) -> StringUtils.isNotBlank(result.command().getMetadata().get(REPLY_TO)))
          .to((key, result, recordContext) -> result.command().getMetadata().get(REPLY_TO),
              Produced.with(Serdes.String(), resultSerde)
                  .withStreamPartitioner((topic, key, value, numPartitions) -> Optional.of(Set.of(0))));

      // Events --> Push
      commandResults
          .filter((key, result) -> result instanceof CommandResult.Success)
          .flatMapValues(result -> ((CommandResult.Success) result).events())
          .filter((key, event) -> event != null)
          .to((key, event, recordContext) -> event.getTopic().value(),
              Produced.with(Serdes.String(), eventSerde));
    }

    /*
     * -------------------------------------------------------------
     * EVENT HANDLING
     * -------------------------------------------------------------
     */

    Set<String> eventTopics = handlers.eventTopics();
    if (!eventTopics.isEmpty()) {
      // --> Events
      KStream<String, Event> events = builder.stream(eventTopics, Consumed.with(Serdes.String(), eventSerde))
          .filter((key, event) -> key != null)
          .filter((key, event) -> event != null)
          .filter((key, event) -> event.getPayload() != null);

      // Events --> Void
      events
          .processValues(() -> new EventProcessor(handlers));
    }

    return builder.build();
  }
}
