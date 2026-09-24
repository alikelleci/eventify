package io.github.alikelleci.eventify.core.kafka.internal;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler;

import java.util.Properties;
import java.util.UUID;

/** The Kafka Streams settings Eventify needs: defaults the application can change, and settings it always sets. */
@Slf4j
public final class StreamsConfigDefaults {

  private StreamsConfigDefaults() {
  }

  /** Sets them in the given configuration. */
  public static void apply(Properties streamsConfig) {
    streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
    streamsConfig.putIfAbsent(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
    streamsConfig.putIfAbsent(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDbConfig.class);
    streamsConfig.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");
    // Always exactly-once: with at-least-once, a retried command would store its events twice.
    alwaysSet(streamsConfig, StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    // Always fail on a processing or send error: continuing would commit half of a command.
    alwaysSet(streamsConfig, StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailProcessingExceptionHandler.class.getName());
    alwaysSet(streamsConfig, StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, DefaultProductionExceptionHandler.class.getName());

    // A unique name per instance (nothing listens on it), so the instance that owns an aggregate can be found.
    String applicationId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG, "eventify");
    streamsConfig.putIfAbsent(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationId + "." + UUID.randomUUID() + ":0");
  }

  private static void alwaysSet(Properties streamsConfig, String name, String value) {
    Object configured = streamsConfig.put(name, value);
    String configuredName = configured instanceof Class<?> type ? type.getName() : String.valueOf(configured);
    if (configured != null && !value.equals(configuredName)) {
      log.warn("'{}' is set by Eventify to '{}'; the configured value '{}' is not used.", name, value, configured);
    }
  }
}
