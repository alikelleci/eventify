package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import io.github.alikelleci.eventify.messaging.Gateway;
import io.github.alikelleci.eventify.messaging.Metadata;
import lombok.SneakyThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface CommandGateway extends Gateway {

  <R> CompletableFuture<R> send(Object payload, Metadata metadata);

  default <R> CompletableFuture<R> send(Object payload) {
    return send(payload, null);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload, Metadata metadata) {
    CompletableFuture<R> future = send(payload, metadata);
    return future.get(1, TimeUnit.MINUTES);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload) {
    return sendAndWait(payload, null);
  }

}
