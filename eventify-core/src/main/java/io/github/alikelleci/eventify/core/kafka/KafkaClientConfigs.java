package io.github.alikelleci.eventify.core.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SecurityConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Kafka client settings copied from another client's configuration. */
public final class KafkaClientConfigs {

  /** Connection settings; the security ones are matched by prefix, as Kafka has dozens. */
  private static final Set<String> CONNECTION_SETTINGS = Set.of(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
      CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
      SecurityConfig.SECURITY_PROVIDERS_CONFIG,
      "config.providers");

  private static final String CONSUMER_PREFIX = "consumer.";

  private KafkaClientConfigs() {
  }

  /** The connection and security settings a consumer needs, not the client's tuning. */
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

  /** The consumer settings of a Streams config, without client id; {@code consumer.} overrides win. */
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
