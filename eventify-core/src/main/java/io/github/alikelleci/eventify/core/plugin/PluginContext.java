package io.github.alikelleci.eventify.core.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.aggregate.AggregateRepository;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;

import java.util.Properties;
import java.util.Set;

/** The read-only view a plugin gets of its Eventify instance. */
public interface PluginContext {

  Properties getStreamsConfig();

  ObjectMapper getObjectMapper();

  /** The running Kafka Streams; {@code null} before Eventify is started. */
  KafkaStreams getKafkaStreams();

  /** The command classes this instance has a command handler for. */
  Set<Class<?>> getCommandClasses();

  /** The command topics of one aggregate. */
  Set<String> getCommandTopics(String aggregateType);

  /** The {@code @AggregateRoot} names of the aggregates this instance handles. */
  Set<String> getAggregateTypes();

  /**
   * Read model of the locally owned aggregates; select a type with {@link AggregateRepository#forType(String)}.
   * Throws InvalidStateStoreException while the stores can't be read, e.g. during rebalancing.
   */
  AggregateRepository getAggregateRepository();

  /** Which instance of the application owns the aggregate, and so has its events; {@code null} when unknown. */
  KeyQueryMetadata getAggregateMetadata(String aggregateId);
}
