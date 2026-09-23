package io.github.alikelleci.eventify.core.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateRepository;
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

  /** The topics of the commands of one aggregate: the commands another aggregate handles are not its own. */
  Set<String> getCommandTopics(String aggregateType);

  /** The names of the aggregates this instance handles, as their {@code @AggregateRoot} gives them. */
  Set<String> getAggregateTypes();

  /**
   * The public read model of locally owned aggregates; select an aggregate type once with
   * {@link AggregateRepository#forType(String)}. See {@link #getAggregateMetadata} for ownership. Stores may be
   * temporarily unavailable while rebalancing.
   *
   * @throws org.apache.kafka.streams.errors.InvalidStateStoreException when the stores cannot be read right now
   */
  AggregateRepository getAggregateRepository();

  /** Which instance of the application owns the aggregate, and so has its events; {@code null} when unknown. */
  KeyQueryMetadata getAggregateMetadata(String aggregateId);
}
