package io.github.alikelleci.eventify.messaging.eventhandling.gateway;

import io.github.alikelleci.eventify.messaging.Gateway;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;

import java.time.Instant;
import java.util.Properties;


public interface EventGateway extends Gateway {

  void publish(Event event);

  default void publish(Object payload, Metadata metadata, Instant timestamp) {
    publish(Event.builder()
        .payload(payload)
        .metadata(metadata)
        .timestamp(timestamp)
        .build());
  }

  default void publish(Object payload, Metadata metadata) {
    publish(payload, metadata, Instant.now());
  }

  default void publish(Object payload) {
    publish(payload, null);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Properties producerConfig;

    public Builder producerConfig(Properties producerConfig) {
      this.producerConfig = producerConfig;
      return this;
    }

    public EventGateway build() {
      return new DefaultEventGateway(this.producerConfig);
    }
  }
}
