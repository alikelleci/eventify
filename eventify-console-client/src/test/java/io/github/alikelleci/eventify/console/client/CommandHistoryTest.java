package io.github.alikelleci.eventify.console.client;

import io.github.alikelleci.eventify.core.Eventify;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** The consumer that reads the commands connects the way the application does, e.g. to a cluster that needs a login. */
@DisplayName("Command history")
class CommandHistoryTest {

  @Test
  @DisplayName("Should give the commands consumer the application's security and consumer settings")
  void theCommandsConsumerUsesTheApplicationsSecurityAndConsumerSettings() {
    Map<String, Object> config = CommandHistory.consumerConfig(Eventify.builder().streamsConfig(streamsConfig()).build());

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
    properties.put("sasl.jaas.config", "jaas");
    properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_BYTES_CONFIG), "1000");
    return properties;
  }
}
