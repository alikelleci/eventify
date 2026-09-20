package io.github.alikelleci.eventify.core.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateReplayer;
import io.github.alikelleci.eventify.core.store.EventStore;
import io.github.alikelleci.eventify.core.store.SnapshotStore;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;

import java.util.Properties;
import java.util.Set;

/**
 * What a plugin sees of the Eventify instance it runs with: its configuration, its Kafka Streams and what it stores.
 * Read only: a plugin doesn't start, stop or change Eventify.
 */
public interface PluginContext {

  Properties getStreamsConfig();

  ObjectMapper getObjectMapper();

  /** The running Kafka Streams; {@code null} before Eventify is started. */
  KafkaStreams getKafkaStreams();

  /** The command classes this instance has a command handler for. */
  Set<Class<?>> getCommandTypes();

  /** The topics of the commands this instance handles. */
  Set<String> getCommandTopics();

  /** The topics of the commands of one aggregate: the commands another aggregate handles are not its own. */
  Set<String> getCommandTopics(String aggregateType);

  /** Rebuilds the state of an aggregate from its events, with the event sourcing handlers of this instance. */
  AggregateReplayer getAggregateReplayer();

  /** The names of the aggregates this instance handles, as their {@code @AggregateRoot} gives them. */
  Set<String> getAggregateTypes();

  /**
   * The stored events of one aggregate on this instance: only those of the aggregates it owns (see
   * {@link #getAggregateMetadata}).
   */
  EventStore getEventStore(String aggregateType);

  /** The stored snapshots of one aggregate on this instance: only those of the aggregates it owns. */
  SnapshotStore getSnapshotStore(String aggregateType);

  /** Which instance of the application owns the aggregate, and so has its events; {@code null} when unknown. */
  KeyQueryMetadata getAggregateMetadata(String aggregateId);
}
