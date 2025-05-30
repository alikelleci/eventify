package io.github.alikelleci.eventify.core.messaging.commandhandling.gateway;

import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;

public interface CommandGateway {

  <R> CompletableFuture<R> send(Command command);

  default <R> CompletableFuture<R> send(Object payload) {
    return send(Command.builder()
        .payload(payload)
        .build());
  }

  @SneakyThrows
  default <R> R sendAndWait(Command command) {
    CompletableFuture<R> future = send(command);
    return future.get(1, TimeUnit.MINUTES);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload) {
    return sendAndWait(Command.builder()
        .payload(payload)
        .build());
  }
}
