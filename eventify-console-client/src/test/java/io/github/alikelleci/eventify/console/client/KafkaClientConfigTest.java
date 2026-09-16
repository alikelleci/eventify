package io.github.alikelleci.eventify.console.client;

import io.github.alikelleci.eventify.core.Eventify;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** The console's Kafka clients connect the way the application does, e.g. to a cluster that needs a login. */
class KafkaClientConfigTest {

  private final Eventify eventify = Eventify.builder().streamsConfig(streamsConfig()).build();

  @Test
  void theRetryProducerUsesTheApplicationsSecurityAndProducerSettings() {
    Map<String, Object> config = ConsoleService.producerConfig(eventify);

    assertThat(config)
        .containsEntry("security.protocol", "SASL_SSL")
        .containsEntry("sasl.mechanism", "PLAIN")
        .containsEntry("sasl.jaas.config", "jaas")
        .containsEntry(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "2000000")
        .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "config-test-console-producer")
        // Only for Kafka Streams' exactly-once processing, not for a single send.
        .doesNotContainKeys(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, ProducerConfig.TRANSACTIONAL_ID_CONFIG);
  }

  @Test
  void theCommandsConsumerUsesTheApplicationsSecurityAndConsumerSettings() {
    Map<String, Object> config = ConsoleService.consumerConfig(eventify);

    assertThat(config)
        .containsEntry("security.protocol", "SASL_SSL")
        .containsEntry("sasl.jaas.config", "jaas")
        .containsEntry(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1000")
        .containsEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
        .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .doesNotContainKey(ConsumerConfig.GROUP_ID_CONFIG);
  }

  private static Properties streamsConfig() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "config-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put("security.protocol", "SASL_SSL");
    properties.put("sasl.mechanism", "PLAIN");
    properties.put("sasl.jaas.config", "jaas");
    properties.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "2000000");
    properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_BYTES_CONFIG), "1000");
    return properties;
  }
}
