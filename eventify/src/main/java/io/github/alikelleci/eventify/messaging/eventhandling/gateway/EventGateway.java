package io.github.alikelleci.eventify.messaging.eventhandling.gateway;

import io.github.alikelleci.eventify.messaging.Gateway;
import io.github.alikelleci.eventify.messaging.Metadata;

import java.time.Instant;
import java.util.Properties;


public interface EventGateway extends Gateway {

  void publish(Object payload, Metadata metadata, Instant timestamp);

  default void publish(Object payload, Metadata metadata) {
    publish(payload, metadata, null);
  }

  default void publish(Object payload) {
    publish(payload, null, null);
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

    public DefaultEventGateway build() {
      return new DefaultEventGateway(this.producerConfig);
    }
  }
}
