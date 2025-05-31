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
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

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
  private final PulsarClient pulsarClient;

  private final List<Consumer<byte[]>> consumers = new ArrayList<>();

  protected PulsarEventify(Properties streamsConfig, ObjectMapper objectMapper, PulsarClient pulsarClient) {
    this.streamsConfig = streamsConfig;
    this.objectMapper = objectMapper;
    this.pulsarClient = pulsarClient;
  }

  @Override
  public void start() {

    /*
     * -------------------------------------------------------------
     * COMMAND HANDLING
     * -------------------------------------------------------------
     */
    if (!getCommandTopics().isEmpty()) {
      PulsarCommandProcessor commandProcessor = new PulsarCommandProcessor(this);

      // create listeners
      for (String topicName : getCommandTopics()) {
        try {
          //TODO what would be sensible retry policies? and what todo if retry exceed?
          consumers.add(pulsarClient.newConsumer(Schema.BYTES)
              .ackTimeout(5, TimeUnit.MINUTES)
              .receiverQueueSize(10)
              .topic(streamsConfig.getProperty(PulsarEventifyConfig.PULSAR_COMMAND_NAMESPACE) + "/" + topicName)
              .subscriptionName("eventify-command-subscription")
              .subscriptionType(SubscriptionType.Key_Shared)
              .replicateSubscriptionState(true)
              .messageListener((consumer, msg) -> {
                try {
                  log.debug("Message received with id: {}", msg.getMessageId());
                  commandProcessor.process(msg);
                  consumer.acknowledge(msg);
                } catch (Exception e) {
                  consumer.negativeAcknowledge(msg);
                }
              })
              .subscribe()
          );
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void stop() {
    log.info("Eventify is shutting down...");

    for (Consumer<byte[]> consumer : consumers) {
      try {
        consumer.close();
      } catch (Exception e) {
        log.error("Error closing consumer", e);
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final List<Object> handlers = new ArrayList<>();

    private Properties streamsConfig;
    private ObjectMapper objectMapper;
    private PulsarClient pulsarClient;

    public Builder registerHandler(Object handler) {
      handlers.add(handler);
      return this;
    }

    public Builder streamsConfig(Properties streamsConfig) {
      this.streamsConfig = streamsConfig;

      if (!streamsConfig.contains(PulsarEventifyConfig.DYNAMODB_TOPIC_EVENT_STORE)) {
        throw new IllegalArgumentException(
            "streamsConfig must contain '" + PulsarEventifyConfig.DYNAMODB_TOPIC_EVENT_STORE + "'");
      }

      if (!streamsConfig.contains(PulsarEventifyConfig.PULSAR_COMMAND_NAMESPACE)) {
        throw new IllegalArgumentException(
            "streamsConfig must contain '" + PulsarEventifyConfig.PULSAR_COMMAND_NAMESPACE + "'");
      }

      return this;
    }

    public Builder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public Builder pulsarClient(PulsarClient pulsarClient) {
      this.pulsarClient = pulsarClient;
      return this;
    }

    public PulsarEventify build() {
      if (objectMapper == null) {
        objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      if (pulsarClient == null) {
        throw new IllegalArgumentException("pulsarClient cannot be null");
      }

      PulsarEventify eventify = new PulsarEventify(streamsConfig, objectMapper, pulsarClient);
      handlers.forEach(handler -> HandlerUtils.registerHandler(eventify, handler));

      return eventify;
    }
  }

}
