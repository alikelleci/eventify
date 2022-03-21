package io.github.alikelleci.eventify.messaging.eventhandling.gateway;

import io.github.alikelleci.eventify.messaging.Gateway;
import io.github.alikelleci.eventify.messaging.Metadata;

import java.util.Properties;


public interface EventGateway extends Gateway {

  void publish(Object payload, Metadata metadata);

  default void publish(Object payload) {
    publish(payload, null);
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
