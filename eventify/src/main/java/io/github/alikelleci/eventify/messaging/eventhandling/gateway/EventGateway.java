package io.github.alikelleci.eventify.messaging.eventhandling.gateway;

import io.github.alikelleci.eventify.messaging.Gateway;
import io.github.alikelleci.eventify.messaging.Metadata;


public interface EventGateway extends Gateway {

  void publish(Object payload, Metadata metadata);

  default void publish(Object payload) {
    publish(payload, null);
  }

}
