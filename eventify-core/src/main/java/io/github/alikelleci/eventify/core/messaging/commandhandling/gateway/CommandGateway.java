package io.github.alikelleci.eventify.core.messaging.commandhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import lombok.SneakyThrows;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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

    /** Producer settings a consumer also knows, but that mean something else for it or must differ. */
    private static final Set<String> NOT_FOR_THE_CONSUMER = Set.of(
        CommonClientConfigs.CLIENT_ID_CONFIG,
        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);

    private Properties producerConfig;
    private Properties consumerConfig;
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

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//    interceptors.add(TracingProducerInterceptor.class.getName());
//
//    this.producerConfig.putIfAbsent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

      return this;
    }

    /**
     * Settings for the consumer that receives the replies, on top of the ones it takes from the producer config (see
     * {@link #build()}). Only needed for a setting the consumer should have differently.
     */
    public CommandGatewayBuilder consumerConfig(Properties consumerConfig) {
      this.consumerConfig = consumerConfig;
      return this;
    }

    /** The producer settings a consumer also has, overridden by {@code overrides} (may be {@code null}). */
    static Properties replyConsumerConfig(Properties producerConfig, Properties overrides) {
      Properties consumerConfig = new Properties();
      Set<String> consumerSettings = ConsumerConfig.configNames();
      producerConfig.forEach((key, value) -> {
        String name = String.valueOf(key);
        if (consumerSettings.contains(name) && !NOT_FOR_THE_CONSUMER.contains(name)) {
          consumerConfig.put(name, value);
        }
      });
      if (overrides != null) {
        consumerConfig.putAll(overrides);
      }
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
     * The reply consumer gets every producer setting a consumer has too: the connection and its security
     * ({@code security.protocol}, {@code sasl.*}, {@code ssl.*}, ...). Without the {@code sasl.*} and {@code ssl.*}
     * settings it couldn't log in on a secured cluster, and every command would time out. Settings given with
     * {@link #consumerConfig(Properties)} go first.
     */
    public DefaultCommandGateway build() {
      Properties consumerConfig = replyConsumerConfig(this.producerConfig, this.consumerConfig);

      if (this.objectMapper == null) {
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      return new DefaultCommandGateway(
          this.producerConfig,
          consumerConfig,
          this.replyTopic,
          this.objectMapper);
    }
  }

}
