package io.github.alikelleci.eventify.core.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SecurityConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka client settings taken from another client's configuration, so a client Eventify adds connects the way the
 * application's own client does. Each method takes a different part: see there.
 */
public final class KafkaClientConfigs {

  /**
   * How to reach the cluster and how to log in to it. The security settings are taken by prefix, not by name: Kafka
   * has over sixty of them, and adds more with every mechanism it supports.
   */
  private static final Set<String> CONNECTION_SETTINGS = Set.of(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
      CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
      SecurityConfig.SECURITY_PROVIDERS_CONFIG,
      "config.providers");

  private static final String CONSUMER_PREFIX = "consumer.";

  private KafkaClientConfigs() {
  }

  /**
   * The settings of a client's configuration (e.g. a producer's) that a consumer needs to connect the same way: how to
   * reach the cluster, and how to log in to it. Not the client's own tuning (timeouts, buffers, metrics, interceptors):
   * the consumer keeps Kafka's defaults for those.
   */
  public static Properties consumerConnectionOf(Properties clientConfig) {
    Properties consumerConfig = new Properties();
    Set<String> consumerSettings = ConsumerConfig.configNames();
    clientConfig.forEach((key, value) -> {
      String name = String.valueOf(key);
      if (consumerSettings.contains(name) && isConnectionSetting(name)) {
        consumerConfig.put(name, value);
      }
    });
    return consumerConfig;
  }

  /**
   * The settings of a Kafka Streams configuration that are also consumer settings; not its client id. Its consumer
   * overrides ({@code consumer.max.poll.records}) win, as they do in Kafka Streams.
   */
  public static Map<String, Object> consumerSettingsOf(Properties streamsConfig) {
    Set<String> consumerSettings = ConsumerConfig.configNames();
    Map<String, Object> config = new HashMap<>();
    streamsConfig.forEach((key, value) -> {
      String name = key.toString();
      if (consumerSettings.contains(name) && !name.equals(ConsumerConfig.CLIENT_ID_CONFIG)) {
        config.put(name, value);
      }
    });
    streamsConfig.forEach((key, value) -> {
      String name = key.toString();
      if (name.startsWith(CONSUMER_PREFIX) && consumerSettings.contains(name.substring(CONSUMER_PREFIX.length()))) {
        config.put(name.substring(CONSUMER_PREFIX.length()), value);
      }
    });
    return config;
  }

  private static boolean isConnectionSetting(String name) {
    return CONNECTION_SETTINGS.contains(name)
        || name.startsWith("sasl.")
        || name.startsWith("ssl.");
  }
}
