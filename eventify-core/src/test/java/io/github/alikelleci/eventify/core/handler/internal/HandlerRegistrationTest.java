package io.github.alikelleci.eventify.core.handler.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.event.annotation.HandleEvent;
import io.github.alikelleci.eventify.core.handler.exception.HandlerRegistrationException;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcast;
import lombok.Value;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** One command handler and one event sourcing handler per type, a second is refused; event handlers can be several. */
@DisplayName("Handler registration")
class HandlerRegistrationTest {

  @Value
  @AggregateRoot("light")
  public static class Light {
    @AggregateId
    String id;
  }

  @Value
  @Topic("commands.light")
  public static class SwitchOn {
    @AggregateId
    String id;
  }

  @Value
  @Topic("events.light")
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

  @Value
  @AggregateRoot("fan")
  public static class Fan {
    @AggregateId
    String id;
  }

  @Value
  @Topic("commands.fan")
  public static class SwitchOnFan {
    @AggregateId
    String id;
  }

  @Value
  @Topic("events.fan")
  public static class FanSwitchedOn {
    @AggregateId
    String id;
  }

  public static class FanHandler {
    @HandleCommand
    public Object handle(SwitchOnFan command, Fan state) {
      return new FanSwitchedOn(command.getId());
    }

    @ApplyEvent
    public Fan apply(FanSwitchedOn event, Fan state) {
      return new Fan(event.getId());
    }
  }

  /** An apply method whose declared result does not match the aggregate state it receives. */
  public static class WrongApplyResultHandler {
    @ApplyEvent
    public Fan apply(SwitchedOn event, Light state) {
      return new Fan(event.getId());
    }
  }

  /** Another aggregate that is called the same as Light. */
  @Value
  @AggregateRoot("light")
  public static class Lamp {
    @AggregateId
    String id;
  }

  public static class LampHandler {
    @HandleCommand
    public Object handle(SwitchOnFan command, Lamp state) {
      return null;
    }
  }

  @Value
  @AggregateRoot("  ")
  public static class Nameless {
    @AggregateId
    String id;
  }

  public static class NamelessHandler {
    @HandleCommand
    public Object handle(SwitchOnFan command, Nameless state) {
      return null;
    }
  }

  public static class HandlerWithoutAggregate {
    @HandleCommand
    public Object handle(SwitchOn command) {
      return null;
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

  /** Annotated on the interface and on its implementation: still one handler. */
  public interface SwitchListener {
    @HandleEvent
    void on(SwitchedOn event);
  }

  public static class AnnotatedTwiceEventHandler implements SwitchListener {
    @HandleEvent
    @Override
    public void on(SwitchedOn event) {
    }
  }

  /** Annotated in the superclass and in the override: still one handler. */
  public static class OverridingEventHandler extends FirstEventHandler {
    @HandleEvent
    @Override
    public void on(SwitchedOn event) {
    }
  }

  public static class HandlerWithoutMessage {
    @HandleEvent
    public void on() {
    }
  }

  @Value
  public static class UnroutedCommand {
    @AggregateId
    String id;
  }

  public static class HandlerWithoutCommandTopic {
    @HandleCommand
    public Object handle(UnroutedCommand command, Light state) {
      return null;
    }
  }

  @Value
  public static class UnroutedEvent {
    @AggregateId
    String id;
  }

  public static class HandlerWithoutEventTopic {
    @HandleEvent
    public void on(UnroutedEvent event) {
    }
  }

  @Test
  @DisplayName("Should refuse a second command handler for the same command")
  void aSecondCommandHandlerForTheSameCommandIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new LightHandler())
        .registerHandler(new OtherCommandHandler())
        .build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining(SwitchOn.class.getName());
  }

  @Test
  @DisplayName("Should refuse a second event sourcing handler for the same event")
  void aSecondEventSourcingHandlerForTheSameEventIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new TwiceApplyingHandler())
        .build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining(SwitchedOn.class.getName());
  }

  @Test
  @DisplayName("Should accept an instance with handlers for several aggregates")
  void severalAggregatesInOneInstanceAreAccepted() {
    Eventify eventify = Eventify.builder().streamsConfig(config())
        .registerHandler(new LightHandler())
        .registerHandler(new FanHandler())
        .build();

    assertThat(eventify.getHandlers().aggregateTypes()).containsExactlyInAnyOrder("light", "fan");
    assertThatCode(eventify::topology).doesNotThrowAnyException();
  }

  /** Their name is what keeps their events and snapshots apart, so it has to be theirs alone. */
  @Test
  @DisplayName("Should refuse two aggregates that are called the same")
  void twoAggregatesWithTheSameNameAreRefused() {
    Eventify eventify = Eventify.builder().streamsConfig(config())
        .registerHandler(new LightHandler())
        .registerHandler(new LampHandler())
        .build();

    assertThatThrownBy(eventify::topology)
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("Light")
        .hasMessageContaining("Lamp")
        .hasMessageContaining("'light'");
  }

  /** The compiler catches a missing name; a blank one it cannot, and a nameless aggregate has no key of its own. */
  @Test
  @DisplayName("Should refuse an aggregate without a name")
  void anAggregateWithoutANameIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new NamelessHandler())
        .build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("Nameless")
        .hasMessageContaining("@AggregateRoot");
  }

  /** Eventify reads the aggregate's events before the handler runs, so it has to know which aggregate that is. */
  @Test
  @DisplayName("Should refuse a command handler that does not take its aggregate")
  void aCommandHandlerWithoutItsAggregateIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new HandlerWithoutAggregate())
        .build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("@AggregateRoot");
  }

  @Test
  @DisplayName("Should refuse an event sourcing handler that returns another aggregate type")
  void anEventSourcingHandlerWithTheWrongAggregateResultIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new WrongApplyResultHandler())
        .build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("@ApplyEvent", "Fan", "Light");
  }

  @Test
  @DisplayName("Should refuse an annotated handler without a message parameter")
  void aHandlerWithoutAMessageParameterIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new HandlerWithoutMessage())
        .build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("@HandleEvent", "first parameter");
  }

  @Test
  @DisplayName("Should refuse command and event handlers whose message has no topic")
  void aHandlerWithoutATopicIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new HandlerWithoutCommandTopic())
        .build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("@HandleCommand", "@Topic");

    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new HandlerWithoutEventTopic())
        .build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("@HandleEvent", "@Topic");
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
  @DisplayName("Should register an event handler once when it is annotated in its interface or superclass too, or registered twice")
  void anEventHandlerIsRegisteredOnce() {
    OverridingEventHandler registeredTwice = new OverridingEventHandler();
    Eventify eventify = Eventify.builder().streamsConfig(config())
        .registerHandler(new AnnotatedTwiceEventHandler())
        .registerHandler(registeredTwice)
        .registerHandler(registeredTwice)
        .build();

    assertThat(eventify.getHandlers().eventHandlers(SwitchedOn.class))
        .<Class<?>>extracting(handler -> handler.getHandler().getClass())
        .containsExactly(AnnotatedTwiceEventHandler.class, OverridingEventHandler.class);
  }

  @Test
  @DisplayName("Should refuse a handler registered after Eventify started")
  void aHandlerRegisteredAfterStartIsRefused() {
    Eventify eventify = Eventify.builder().streamsConfig(config())
        .registerHandler(new LightHandler())
        .build();
    eventify.start();
    try {
      assertThatThrownBy(() -> eventify.registerHandler(new FirstEventHandler()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("started");
    } finally {
      eventify.stop();
    }
  }

  public static class UnsupportedParameterHandler {
    @HandleEvent
    public void on(SwitchedOn event, String unsupported) {
    }
  }

  public static class TwoParameterUpcaster {
    @Upcast(type = "com.example.SwitchedOn", revision = 1)
    public JsonNode upcast(ObjectNode payload, String unsupported) {
      return payload;
    }
  }

  @Test
  @DisplayName("Should refuse a handler with a parameter Eventify has no value for")
  void aHandlerWithAnUnsupportedParameterIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new UnsupportedParameterHandler())
        .build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("@HandleEvent", "java.lang.String");
  }

  @Test
  @DisplayName("Should refuse an upcaster that does not take and return the payload as JSON")
  void anUpcasterWithTheWrongSignatureIsRefused() {
    assertThatThrownBy(() -> Eventify.builder().streamsConfig(config())
        .registerHandler(new TwoParameterUpcaster())
        .build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("@Upcast");
  }

  private static Properties config() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "handler-registration-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return properties;
  }
}
