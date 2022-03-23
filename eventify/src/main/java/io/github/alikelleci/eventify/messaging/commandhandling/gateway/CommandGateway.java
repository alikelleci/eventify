package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import io.github.alikelleci.eventify.messaging.Gateway;
import io.github.alikelleci.eventify.messaging.Metadata;
import lombok.SneakyThrows;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface CommandGateway extends Gateway {

  <R> CompletableFuture<R> send(Object payload, Metadata metadata, Instant timestamp);

  default <R> CompletableFuture<R> send(Object payload, Metadata metadata) {
    return send(payload, metadata, null);
  }

  default <R> CompletableFuture<R> send(Object payload) {
    return send(payload, null, null);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload, Metadata metadata, Instant timestamp) {
    CompletableFuture<R> future = send(payload, metadata, timestamp);
    return future.get(1, TimeUnit.MINUTES);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload, Metadata metadata) {
    return sendAndWait(payload, metadata, null);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload) {
    return sendAndWait(payload, null, null);
  }

  public static CommandGatewayBuilder builder() {
    return new CommandGatewayBuilder();
  }

  public static class CommandGatewayBuilder {

    private Properties producerConfig;
    private Properties consumerConfig;
    private String replyTopic;

    public CommandGatewayBuilder producerConfig(Properties producerConfig) {
      this.producerConfig = producerConfig;
      return this;
    }

    public CommandGatewayBuilder consumerConfig(Properties consumerConfig) {
      this.consumerConfig = consumerConfig;
      return this;
    }

    public CommandGatewayBuilder replyTopic(String replyTopic) {
      this.replyTopic = replyTopic;
      return this;
    }

    public DefaultCommandGateway build() {
      return new DefaultCommandGateway(this.producerConfig, this.consumerConfig, this.replyTopic);
    }
  }

}
