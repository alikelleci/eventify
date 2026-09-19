package io.github.alikelleci.eventify.core.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** The consumer that receives the replies connects the way the producer does, also to a secured cluster. */
@DisplayName("Kafka client configurations")
class KafkaClientConfigsTest {

  @Test
  @DisplayName("Should give a consumer the connection and security settings of a producer, and nothing else")
  void theReplyConsumerConnectsLikeTheProducer() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9093");
    producerConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    producerConfig.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";");
    producerConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/truststore.jks");
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "orders-api");
    producerConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.example.ProducerInterceptor");
    producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000");

    Properties consumerConfig = KafkaClientConfigs.consumerConnectionOf(producerConfig);

    assertThat(consumerConfig)
        .containsEntry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker:9093")
        .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
        .containsEntry(SaslConfigs.SASL_MECHANISM, "PLAIN")
        .containsKey(SaslConfigs.SASL_JAAS_CONFIG)
        .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/truststore.jks")
        .doesNotContainKey(ProducerConfig.ACKS_CONFIG)
        .doesNotContainKey(CommonClientConfigs.CLIENT_ID_CONFIG)
        .doesNotContainKey(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)
        // The producer's tuning is the producer's: the consumer keeps Kafka's defaults.
        .doesNotContainKey(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
  }

  @Test
  @DisplayName("Should give a consumer the consumer settings of Kafka Streams, with its consumer overrides, without its client id")
  void theConsumerSettingsOfKafkaStreams() {
    Properties streamsConfig = new Properties();
    streamsConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9093");
    streamsConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    streamsConfig.put("consumer." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
    streamsConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "orders");
    streamsConfig.put("application.id", "orders");

    Map<String, Object> consumerConfig = KafkaClientConfigs.consumerSettingsOf(streamsConfig);

    assertThat(consumerConfig)
        .containsEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9093")
        .containsEntry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50")
        .doesNotContainKey(ConsumerConfig.CLIENT_ID_CONFIG)
        .doesNotContainKey("application.id");
  }
}
