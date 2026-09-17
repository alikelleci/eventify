package io.github.alikelleci.eventify.core.messaging.commandhandling.gateway;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** The consumer that receives the replies connects the way the producer does, also to a secured cluster. */
@DisplayName("Command gateway reply consumer configuration")
class CommandGatewayConfigTest {

  @Test
  @DisplayName("Should give the reply consumer the producer's connection and security settings, and nothing else")
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

    Properties consumerConfig = CommandGateway.CommandGatewayBuilder.replyConsumerConfig(producerConfig);

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
}
