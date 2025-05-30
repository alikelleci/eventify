package io.github.alikelleci.eventify.core.messaging.eventhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import java.util.Properties;

public class PulsarEventGateway implements EventGateway {

  protected PulsarEventGateway(Properties producerConfig, ObjectMapper objectMapper) {

  }

  @Override
  public void publish(Event event) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Properties producerConfig;
    private ObjectMapper objectMapper;

    public Builder producerConfig(Properties producerConfig) {
      return this;
    }

    public Builder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public PulsarEventGateway build() {
      if (this.objectMapper == null) {
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      return new PulsarEventGateway(
          this.producerConfig,
          this.objectMapper);
    }
  }
}
