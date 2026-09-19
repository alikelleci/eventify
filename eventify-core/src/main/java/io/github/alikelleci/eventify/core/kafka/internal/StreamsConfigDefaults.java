package io.github.alikelleci.eventify.core.kafka.internal;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

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
    // Always exactly-once: a command's events, its result and the event store are written in one transaction. With
    // at-least-once, a command handled again after a crash would add its events a second time, under the next sequences.
    Object guarantee = streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    if (guarantee != null && !StreamsConfig.EXACTLY_ONCE_V2.equals(guarantee)) {
      log.warn("'{}' is set by Eventify to '{}'; the configured value '{}' is not used.", StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2, guarantee);
    }
    streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
    streamsConfig.putIfAbsent(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
    streamsConfig.putIfAbsent(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDbConfig.class);
    streamsConfig.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");

    // A unique name for this instance, not an address: nothing listens on it. Kafka Streams shares it with the
    // other instances, so each one can tell which instance owns a key (used by the console to route queries).
    // Always set here: two instances with the same name would be taken for one.
    String applicationId = streamsConfig.getProperty(StreamsConfig.APPLICATION_ID_CONFIG, "eventify");
    Object configured = streamsConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationId + "." + UUID.randomUUID() + ":0");
    if (configured != null) {
      log.warn("'{}' is set by Eventify; the configured value '{}' is not used.", StreamsConfig.APPLICATION_SERVER_CONFIG, configured);
    }
  }
}
