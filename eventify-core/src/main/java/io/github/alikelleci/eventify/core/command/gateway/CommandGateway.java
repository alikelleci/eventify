package io.github.alikelleci.eventify.core.command.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.gateway.internal.DefaultCommandGateway;
import io.github.alikelleci.eventify.core.kafka.KafkaClientConfigs;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
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
      Properties consumerConfig = KafkaClientConfigs.consumerConnectionOf(this.producerConfig);

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
