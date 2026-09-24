package io.github.alikelleci.eventify.core.command.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.EventifyException;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.command.exception.CommandExecutionException;
import io.github.alikelleci.eventify.core.command.exception.CommandTimeoutException;
import io.github.alikelleci.eventify.core.command.gateway.internal.DefaultCommandGateway;
import io.github.alikelleci.eventify.core.kafka.KafkaClientConfigs;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface CommandGateway extends AutoCloseable {

  /** Sends the command; the future fails with {@link CommandExecutionException} when it is rejected. */
  CompletableFuture<CommandResult.Success> send(Command command);

  /** Releases the gateway's Kafka clients and thread. */
  @Override
  void close();

  default CompletableFuture<CommandResult.Success> send(Object payload) {
    return send(Command.builder()
        .payload(payload)
        .build());
  }

  /**
   * Sends the command and waits for its result. Throws {@link CommandExecutionException} when rejected,
   * {@link CommandTimeoutException} on timeout (it may still be handled), {@link EventifyException} when interrupted.
   */
  default CommandResult.Success sendAndWait(Command command, long timeout, TimeUnit unit) {
    CompletableFuture<CommandResult.Success> future = send(command);
    try {
      return future.get(timeout, unit);
    } catch (ExecutionException e) {
      throw resultFailure(command, e.getCause());
    } catch (TimeoutException e) {
      throw new CommandTimeoutException("No result for command " + command.getId() + " within " + timeout + " " + unit.name().toLowerCase(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EventifyException("Interrupted while waiting for the result of command " + command.getId(), e);
    }
  }

  default CommandResult.Success sendAndWait(Object payload, long timeout, TimeUnit unit) {
    return sendAndWait(Command.builder()
        .payload(payload)
        .build(), timeout, unit);
  }

  default CommandResult.Success sendAndWait(Command command) {
    return sendAndWait(command, 1, TimeUnit.MINUTES);
  }

  default CommandResult.Success sendAndWait(Object payload) {
    return sendAndWait(payload, 1, TimeUnit.MINUTES);
  }

  /** The failure as unchecked exception; a gateway timeout becomes a {@link CommandTimeoutException}. */
  private static RuntimeException resultFailure(Command command, Throwable failure) {
    if (failure instanceof TimeoutException) {
      return new CommandTimeoutException("No result for command " + command.getId() + ": " + failure.getMessage(), failure);
    }
    if (failure instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    return new EventifyException("Command " + command.getId() + " failed: " + failure.getMessage(), failure);
  }

  public static CommandGatewayBuilder builder() {
    return new CommandGatewayBuilder();
  }

  public static class CommandGatewayBuilder {

    private Properties producerConfig;
    private String replyTopic;
    private ObjectMapper objectMapper;

    public CommandGatewayBuilder producerConfig(Properties producerConfig) {
      this.producerConfig = new Properties();
      this.producerConfig.putAll(producerConfig);
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

    /** The reply consumer takes the producer's connection and security settings. */
    public DefaultCommandGateway build() {
      Properties consumerConfig = KafkaClientConfigs.consumerConnectionOf(this.producerConfig);

      if (this.objectMapper == null) {
        this.objectMapper = EventifyObjectMapper.create();
      }

      return new DefaultCommandGateway(
          this.producerConfig,
          consumerConfig,
          this.replyTopic,
          this.objectMapper);
    }
  }

}
