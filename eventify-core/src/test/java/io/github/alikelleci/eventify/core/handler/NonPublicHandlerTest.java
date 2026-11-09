package io.github.alikelleci.eventify.core.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.event.annotation.EventHandler;
import io.github.alikelleci.eventify.core.handler.exception.HandlerRegistrationException;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Revision;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.Matchers;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcaster;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Handlers in a package-private class or with package-private methods, as a Spring {@code @Component class} often has. */
@DisplayName("Non-public handlers")
class NonPublicHandlerTest {

  @AggregateRoot("lamp")
  public record Lamp(@AggregateId String id, int switches) {
  }

  @Topic("commands.lamp")
  public record Toggle(@AggregateId String id) {
  }

  @Topic("events.lamp")
  @Revision(2)
  public record Toggled(@AggregateId String id, String by) {
  }

  @Topic("commands.alarm")
  public record Ring(@AggregateId String id) {
  }

  /** Package-private class, public methods. */
  static class PackagePrivateClass {
    @CommandHandler
    public Toggled handle(Toggle command, Lamp state) {
      return new Toggled(command.id(), "class");
    }

    @EventSourcingHandler
    public Lamp on(Toggled event, Lamp state) {
      return new Lamp(event.id(), state == null ? 1 : state.switches() + 1);
    }
  }

  /** Public class, package-private methods. */
  public static class PackagePrivateMethods {
    @CommandHandler
    Toggled handle(Toggle command, Lamp state) {
      return new Toggled(command.id(), "methods");
    }

    @EventSourcingHandler
    Lamp on(Toggled event, Lamp state) {
      return new Lamp(event.id(), state == null ? 1 : state.switches() + 1);
    }
  }

  static class PackagePrivateEventHandler {
    @EventHandler
    Ring on(Toggled event) {
      return new Ring(event.id());
    }
  }

  static class PackagePrivateUpcaster {
    @Upcaster(type = "io.github.alikelleci.eventify.core.handler.NonPublicHandlerTest$Toggled", revision = 1)
    JsonNode addBy(ObjectNode payload) {
      return payload.put("by", "upcaster");
    }
  }

  public static class PrivateCommandHandler {
    @CommandHandler
    private Toggled handle(Toggle command, Lamp state) {
      return null;
    }
  }

  public static class PrivateEventSourcingHandler {
    @EventSourcingHandler
    private Lamp on(Toggled event, Lamp state) {
      return state;
    }
  }

  public static class PrivateEventHandler {
    @EventHandler
    private void on(Toggled event) {
    }
  }

  public static class PrivateUpcaster {
    @Upcaster(type = "io.github.alikelleci.eventify.core.handler.NonPublicHandlerTest$Toggled", revision = 1)
    private JsonNode addBy(ObjectNode payload) {
      return payload;
    }
  }

  @Test
  @DisplayName("Should handle commands and replay events with handlers in a package-private class")
  void handlersInAPackagePrivateClassAreCalled() {
    assertThat(toggleTwice(new PackagePrivateClass())).containsExactly("success null", "success null");
  }

  @Test
  @DisplayName("Should handle commands and replay events with package-private handler methods")
  void packagePrivateHandlerMethodsAreCalled() {
    assertThat(toggleTwice(new PackagePrivateMethods())).containsExactly("success null", "success null");
  }

  @Test
  @DisplayName("Should send the command a package-private event handler returns")
  void aPackagePrivateEventHandlerIsCalled() {
    try (TopologyTestDriver driver = new TopologyTestDriver(eventify(new PackagePrivateEventHandler()).topology())) {
      driver.createInputTopic("events.lamp", new StringSerializer(), new EventSerde().serializer())
          .pipeInput("lamp-1", Event.builder().aggregateType("lamp").payload(new Toggled("lamp-1", "test")).sequence(1).build());

      assertThat(driver.createOutputTopic("commands.alarm", new StringDeserializer(), new CommandSerde().deserializer()).readValuesToList())
          .extracting(Command::getPayload)
          .containsExactly(new Ring("lamp-1"));
    }
  }

  @Test
  @DisplayName("Should upcast with a package-private upcaster")
  void aPackagePrivateUpcasterIsCalled() throws Exception {
    ObjectMapper objectMapper = EventifyObjectMapper.create();
    ObjectNode json = objectMapper.valueToTree(Event.builder().aggregateType("lamp").payload(new Toggled("lamp-1", null)).sequence(1).build());
    json.put("revision", 1);

    Event event = new EventSerde().withUpcasters(new PackagePrivateUpcaster()).deserializer()
        .deserialize("events.lamp", objectMapper.writeValueAsBytes(json));

    assertThat(event.getPayload()).isEqualTo(new Toggled("lamp-1", "upcaster"));
  }

  @Test
  @DisplayName("Should refuse a private handler method of every kind: through a Spring proxy it would run without the bean's fields")
  void privateHandlerMethodsAreRefused() {
    for (Object handler : new Object[]{new PrivateCommandHandler(), new PrivateEventSourcingHandler(), new PrivateEventHandler(), new PrivateUpcaster()}) {
      assertThatThrownBy(() -> eventify(handler))
          .as(handler.getClass().getSimpleName())
          .isInstanceOf(HandlerRegistrationException.class)
          .hasMessageContaining("private");
    }
  }

  /** Two commands: the second replays the event of the first. */
  private static java.util.List<String> toggleTwice(Object handler) {
    try (TopologyTestDriver driver = new TopologyTestDriver(eventify(handler).topology())) {
      var commands = driver.createInputTopic("commands.lamp", new StringSerializer(), new CommandSerde().serializer());
      TestOutputTopic<String, CommandResult> results = driver.createOutputTopic("commands.lamp.results",
          new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
      commands.pipeInput("lamp-1", Command.builder().payload(new Toggle("lamp-1")).build());
      commands.pipeInput("lamp-1", Command.builder().payload(new Toggle("lamp-1")).build());
      return results.readValuesToList().stream()
          .map(result -> Matchers.outcome(result) + " " + Matchers.causeOf(result))
          .toList();
    }
  }

  private static Eventify eventify(Object handler) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "non-public-handler-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return Eventify.builder().streamsConfig(properties).registerHandler(handler).build();
  }
}
