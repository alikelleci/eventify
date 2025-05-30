package io.github.alikelleci.eventify.engine.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.core.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.core.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.core.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.core.support.serialization.json.JacksonUtils;
import io.github.alikelleci.eventify.core.util.HandlerUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

@Slf4j
@Getter
public class PulsarEventify implements Eventify {

  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers = new HashMap<>();
  private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<String, Upcaster> upcasters = new ArrayListValuedHashMap<>();

  private final Properties streamsConfig;
  private final ObjectMapper objectMapper;

  protected PulsarEventify(Properties streamsConfig, ObjectMapper objectMapper) {
    this.streamsConfig = streamsConfig;
    this.objectMapper = objectMapper;
  }

  public static Builder builder() {
    return new Builder();
  }

  public void start() {

  }

  public void stop() {
    log.info("Eventify is shutting down...");
  }

  public static class Builder {

    private final List<Object> handlers = new ArrayList<>();

    private Properties streamsConfig;
    private ObjectMapper objectMapper;

    public Builder registerHandler(Object handler) {
      handlers.add(handler);
      return this;
    }

    public Builder streamsConfig(Properties streamsConfig) {
      this.streamsConfig = streamsConfig;
      return this;
    }

    public Builder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public PulsarEventify build() {
      if (this.objectMapper == null) {
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      PulsarEventify eventify = new PulsarEventify(
          this.streamsConfig,
          this.objectMapper
      );

      this.handlers.forEach(handler -> HandlerUtils.registerHandler(eventify, handler));

      return eventify;
    }

  }

}
