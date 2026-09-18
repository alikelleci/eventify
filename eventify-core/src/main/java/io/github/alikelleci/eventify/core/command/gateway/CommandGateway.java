package io.github.alikelleci.eventify.core.command.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.gateway.internal.DefaultCommandGateway;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface CommandGateway extends AutoCloseable {

  <R> CompletableFuture<R> send(Command command);

  /** Releases the gateway's Kafka clients and thread. */
  @Override
  void close();

  default <R> CompletableFuture<R> send(Object payload) {
    return send(Command.builder()
        .payload(payload)
        .build());
  }

  @SneakyThrows
  default <R> R sendAndWait(Command command, long timeout, TimeUnit unit) {
    CompletableFuture<R> future = send(command);
    return future.get(timeout, unit);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload, long timeout, TimeUnit unit) {
    return sendAndWait(Command.builder()
        .payload(payload)
        .build(), timeout, unit);
  }

  @SneakyThrows
  default <R> R sendAndWait(Command command) {
    return sendAndWait(command, 1, TimeUnit.MINUTES);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload) {
    return sendAndWait(payload, 1, TimeUnit.MINUTES);
  }

  public static CommandGatewayBuilder builder() {
    return new CommandGatewayBuilder();
  }

  public static class CommandGatewayBuilder {

    /**
     * What the consumer that receives the results takes from the producer configuration: how to reach the cluster, and
     * how to log in to it. Not the producer's own tuning (timeouts, buffers, metrics, interceptors).
     *
     * <p>The security settings are taken by prefix, not by name: Kafka has over sixty of them, and adds more with every
     * mechanism it supports.
     */
    private static final Set<String> CONNECTION_SETTINGS = Set.of(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        SecurityConfig.SECURITY_PROVIDERS_CONFIG,
        "config.providers");

    private static boolean isConnectionSetting(String name) {
      return CONNECTION_SETTINGS.contains(name)
          || name.startsWith("sasl.")
          || name.startsWith("ssl.");
    }

    private Properties producerConfig;
    private String replyTopic;
    private ObjectMapper objectMapper;

    public CommandGatewayBuilder producerConfig(Properties producerConfig) {
      this.producerConfig = producerConfig;
      this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
      this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
      this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      this.producerConfig.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

      return this;
    }

    /** How the consumer that receives the results reaches the cluster, taken from the producer configuration. */
    static Properties replyConsumerConfig(Properties producerConfig) {
      Properties consumerConfig = new Properties();
      Set<String> consumerSettings = ConsumerConfig.configNames();
      producerConfig.forEach((key, value) -> {
        String name = String.valueOf(key);
        if (consumerSettings.contains(name) && isConnectionSetting(name)) {
          consumerConfig.put(name, value);
        }
      });
      return consumerConfig;
    }

    public CommandGatewayBuilder replyTopic(String replyTopic) {
      this.replyTopic = replyTopic;
      return this;
    }

    public CommandGatewayBuilder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    /**
     * The consumer that receives the results needs no configuration of its own: it takes every producer setting that a
     * consumer has too, so it connects the way the producer does. That includes the security settings
     * ({@code security.protocol}, {@code sasl.*}, {@code ssl.*}): without them it couldn't log in on a secured cluster,
     * and every command would wait for its result until it timed out.
     */
    public DefaultCommandGateway build() {
      Properties consumerConfig = replyConsumerConfig(this.producerConfig);

      if (this.objectMapper == null) {
        this.objectMapper = EventifyObjectMapper.get();
      }

      return new DefaultCommandGateway(
          this.producerConfig,
          consumerConfig,
          this.replyTopic,
          this.objectMapper);
    }
  }

}
