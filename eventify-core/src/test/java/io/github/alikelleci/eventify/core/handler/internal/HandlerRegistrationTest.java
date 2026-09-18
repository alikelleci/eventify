package io.github.alikelleci.eventify.core.handler.internal;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.event.annotation.HandleEvent;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import lombok.Value;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A command and an event have one command handler and one event sourcing handler: a second one would replace the
 * first, without a word. Event handlers can be several.
 */
@DisplayName("Handler registration")
class HandlerRegistrationTest {

  @Value
  @AggregateRoot
  public static class Light {
    @AggregateId
    String id;
  }

  @Value
  public static class SwitchOn {
    @AggregateId
    String id;
  }

  @Value
  public static class SwitchedOn {
    @AggregateId
    String id;
  }

  public static class LightHandler {
    @HandleCommand
    public Object handle(SwitchOn command, Light state) {
      return new SwitchedOn(command.getId());
    }

    @ApplyEvent
    public Light apply(SwitchedOn event, Light state) {
      return new Light(event.getId());
    }
  }

  /** Handles the same command as LightHandler. */
  public static class OtherCommandHandler {
    @HandleCommand
    public Object handle(SwitchOn command, Light state) {
      return null;
    }
  }

  /** Two event sourcing handlers for the same event in one class. */
  public static class TwiceApplyingHandler {
    @ApplyEvent
    public Light apply(SwitchedOn event, Light state) {
      return new Light(event.getId());
    }

    @ApplyEvent
    public Light apply(SwitchedOn event, Light state, Metadata metadata) {
      return new Light(event.getId());
    }
  }

  /** Overrides its superclass's handlers: still one handler each. */
  public static class OverridingHandler extends LightHandler {
    @Override
    @HandleCommand
    public Object handle(SwitchOn command, Light state) {
      return new SwitchedOn(command.getId());
    }

    @Override
    @ApplyEvent
    public Light apply(SwitchedOn event, Light state) {
      return new Light(event.getId());
    }
  }

  public static class FirstEventHandler {
    @HandleEvent
    public void on(SwitchedOn event) {
    }
  }

  public static class SecondEventHandler {
    @HandleEvent
    public void on(SwitchedOn event) {
    }
  }

  @Test
  @DisplayName("Should refuse a second command handler for the same command")
  void aSecondCommandHandlerForTheSameCommandIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new LightHandler())
        .registerHandler(new OtherCommandHandler())
        .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(SwitchOn.class.getName());
  }

  @Test
  @DisplayName("Should refuse a second event sourcing handler for the same event")
  void aSecondEventSourcingHandlerForTheSameEventIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new TwiceApplyingHandler())
        .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(SwitchedOn.class.getName());
  }

  @Test
  @DisplayName("Should accept a handler that overrides its superclass's handlers, and registering the same handler twice")
  void anOverridingHandlerAndTheSameHandlerTwiceAreAccepted() {
    OverridingHandler handler = new OverridingHandler();

    Eventify eventify = Eventify.builder().streamsConfig(config())
        .registerHandler(handler)
        .registerHandler(handler)
        .build();

    assertThat(eventify.getHandlers().commandHandlers()).containsOnlyKeys(SwitchOn.class);
    assertThat(eventify.getHandlers().eventSourcingHandlers()).containsOnlyKeys(SwitchedOn.class);
  }

  @Test
  @DisplayName("Should accept several event handlers for the same event")
  void severalEventHandlersForTheSameEventAreAccepted() {
    Eventify eventify = Eventify.builder().streamsConfig(config())
        .registerHandler(new FirstEventHandler())
        .registerHandler(new SecondEventHandler())
        .build();

    assertThat(eventify.getHandlers().eventHandlers(SwitchedOn.class)).hasSize(2);
  }

  @Test
  @DisplayName("Should refuse a handler registered after Eventify started")
  void aHandlerRegisteredAfterStartIsRefused() {
    // Without @Topic nothing is subscribed: start() returns without connecting to Kafka.
    Eventify eventify = Eventify.builder().streamsConfig(config())
        .registerHandler(new LightHandler())
        .build();
    eventify.start();

    assertThatThrownBy(() -> eventify.registerHandler(new FirstEventHandler()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("started");
  }

  private static Properties config() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "handler-registration-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return properties;
  }
}
