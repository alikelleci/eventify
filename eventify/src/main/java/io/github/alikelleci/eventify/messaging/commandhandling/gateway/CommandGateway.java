package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import io.github.alikelleci.eventify.messaging.Gateway;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import lombok.SneakyThrows;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface CommandGateway extends Gateway {

  <R> CompletableFuture<R> send(Command command);

  default <R> CompletableFuture<R> send(Object payload, Metadata metadata, Instant timestamp) {
    return send(Command.builder()
        .payload(payload)
        .metadata(metadata)
        .timestamp(timestamp)
        .build());
  }

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
    return sendAndWait(payload, metadata ,null);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload) {
    return sendAndWait(payload, null ,null);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Properties producerConfig;
    private Properties consumerConfig;
    private String replyTopic;

    public Builder producerConfig(Properties producerConfig) {
      this.producerConfig = producerConfig;
      return this;
    }

    public Builder consumerConfig(Properties consumerConfig) {
      this.consumerConfig = consumerConfig;
      return this;
    }

    public Builder replyTopic(String replyTopic) {
      this.replyTopic = replyTopic;
      return this;
    }

    public DefaultCommandGateway build() {
      return new DefaultCommandGateway(this.producerConfig, this.consumerConfig, this.replyTopic);
    }
  }

}
