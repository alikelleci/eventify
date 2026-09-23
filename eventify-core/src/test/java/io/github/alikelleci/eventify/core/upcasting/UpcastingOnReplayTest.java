package io.github.alikelleci.eventify.core.upcasting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Revision;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcast;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Eventify's upcasters are used when an aggregate is rebuilt; the chain itself: see UpcastingChainTest. */
@DisplayName("Upcasting on replay")
class UpcastingOnReplayTest {

  private static final String REGISTERED = "io.github.alikelleci.eventify.core.upcasting.UpcastingOnReplayTest$Registered";

  @Topic("commands.profile")
  public record Greet(@AggregateId String id) {
  }

  @Topic("events.profile")
  public interface ProfileEvent {
  }

  /** Revision 1 had {@code fullName}; revision 2 calls it {@code name}. */
  @Revision(2)
  public record Registered(@AggregateId String id, String name) implements ProfileEvent {
  }

  public record Greeted(@AggregateId String id, String greeting) implements ProfileEvent {
  }

  @AggregateRoot("profile")
  public record Profile(@AggregateId String id, String name) {
  }

  public static class ProfileHandler {
    @HandleCommand
    public Greeted handle(Greet command, Profile state) {
      return new Greeted(command.id(), "Hello " + state.name());
    }

    @ApplyEvent
    public Profile apply(Registered event, Profile state) {
      return new Profile(event.id(), event.name());
    }
  }

  public static class ProfileUpcaster {
    @Upcast(type = REGISTERED, revision = 1)
    public JsonNode renameFullName(ObjectNode payload) {
      payload.set("name", payload.remove("fullName"));
      return payload;
    }
  }

  private TopologyTestDriver driver;

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("Should upcast a stored event of an older revision when a command rebuilds the aggregate")
  void aStoredEventIsUpcastWhenTheAggregateIsRebuilt() {
    assertThat(greetingAfterRebuild(new ProfileHandler(), new ProfileUpcaster())).isEqualTo("Hello Ada");
  }

  @Test
  @DisplayName("Should read a stored event as it was written without an upcaster")
  void withoutAnUpcasterTheStoredEventIsNotUpcast() {
    // Shows that the name only comes from the upcaster: without it, the old field is not read.
    assertThat(greetingAfterRebuild(new ProfileHandler())).isEqualTo("Hello null");
  }

  /** Stores {@code Registered} as revision 1 wrote it, then greets: the greeting shows the name the replay read. */
  private String greetingAfterRebuild(Object... handlers) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "upcasting-on-replay-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    Eventify.EventifyBuilder builder = Eventify.builder().streamsConfig(properties);
    for (Object handler : handlers) {
      builder.registerHandler(handler);
    }
    driver = new TopologyTestDriver(builder.build().topology());
    TestInputTopic<String, Command> commands = driver.createInputTopic("commands.profile", new StringSerializer(), new CommandSerde().serializer());
    TestOutputTopic<String, Event> events = driver.createOutputTopic("events.profile", new StringDeserializer(), new EventSerde().deserializer());

    Event current = Event.builder().aggregateType("profile").payload(new Registered("ada", "Ada")).sequence(1).build();
    ObjectNode stored = EventifyObjectMapper.create().valueToTree(current);
    ObjectNode payload = (ObjectNode) stored.get("payload");
    payload.set("fullName", payload.remove("name"));
    stored.put("revision", 1);
    // The event store's own serde writes it: the JSON as an older version of the application stored it.
    KeyValueStore<String, Object> eventStore = driver.getKeyValueStore("event-store");
    eventStore.put(StoreKeys.of("profile", "ada", 1), stored);

    Command greet = Command.builder().payload(new Greet("ada")).build();
    commands.pipeInput(greet.getAggregateId(), greet);

    return ((Greeted) events.readValue().getPayload()).greeting();
  }
}
