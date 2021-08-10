package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import io.github.alikelleci.eventify.messaging.Gateway;
import io.github.alikelleci.eventify.messaging.Metadata;

public interface CommandGateway extends Gateway {

  void send(Object payload, Metadata metadata);

  default void send(Object payload) {
    send(payload, null);
  }

}
