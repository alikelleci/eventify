package io.github.alikelleci.eventify.core.messaging.eventhandling.gateway;

import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;


public interface EventGateway {

  void publish(Event event);

  default void publish(Object payload) {
    publish(Event.builder()
        .payload(payload)
        .build());
  }
}
