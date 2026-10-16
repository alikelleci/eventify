package io.github.alikelleci.eventify.core.command.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.EventifyException;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.command.exception.CommandTimeoutException;
import io.github.alikelleci.eventify.core.command.gateway.internal.DefaultCommandGateway;
import io.github.alikelleci.eventify.core.kafka.KafkaClientConfigs;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface CommandGateway extends AutoCloseable {

  /** Sends a command. The future fails when it is rejected. */
  CompletableFuture<List<Event>> send(Command command);

  /** Releases gateway resources. */
  @Override
  void close();

  default CompletableFuture<List<Event>> send(Object payload) {
    return send(Command.builder()
        .payload(payload)
        .build());
  }

  /** Sends a command and waits for its result. A timeout does not mean the command was not handled. */
  default List<Event> sendAndWait(Command command, long timeout, TimeUnit unit) {
    CompletableFuture<List<Event>> future = send(command);
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

  default List<Event> sendAndWait(Object payload, long timeout, TimeUnit unit) {
    return sendAndWait(Command.builder()
        .payload(payload)
        .build(), timeout, unit);
  }

  default List<Event> sendAndWait(Command command) {
    return sendAndWait(command, 1, TimeUnit.MINUTES);
  }

  default List<Event> sendAndWait(Object payload) {
    return sendAndWait(payload, 1, TimeUnit.MINUTES);
  }

  /** Converts an asynchronous failure to the public exception type. */
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

    /** Builds a gateway using the producer's connection settings. */
    public CommandGateway build() {
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
