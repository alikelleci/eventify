package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.AggregateRoot;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Immutability is recommended, not required: an aggregate and its events written in a mutable style still give the
 * events the handler returned, and every accepted command gets its result.
 */
@DisplayName("Mutable aggregates and command results")
class MutableAggregateTest {

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @AggregateRoot
  public static class Cart {
    @AggregateId
    String id;
    List<String> items = new ArrayList<>();
  }

  @TopicInfo("commands.cart")
  public interface CartCommand {
  }

  @TopicInfo("events.cart")
  public interface CartEvent {
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class AddItem implements CartCommand {
    @AggregateId
    String id;
    String item;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CheckOut implements CartCommand {
    @AggregateId
    String id;
  }

  /** Accepted, without anything to change. */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Touch implements CartCommand {
    @AggregateId
    String id;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ItemAdded implements CartEvent {
    @AggregateId
    String id;
    String item;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CheckedOut implements CartEvent {
    @AggregateId
    String id;
    List<String> items;
  }

  public static class CartHandler {
    @HandleCommand
    public Object handle(AddItem command, Cart state) {
      return new ItemAdded(command.getId(), command.getItem());
    }

    /** The event gets the aggregate's own list, not a copy. */
    @HandleCommand
    public Object handle(CheckOut command, Cart state) {
      return new CheckedOut(command.getId(), state.getItems());
    }

    @HandleCommand
    public Object handle(Touch command, Cart state) {
      return null;
    }

    @ApplyEvent
    public Cart apply(ItemAdded event, Cart state) {
      Cart cart = state != null ? state : new Cart(event.getId(), new ArrayList<>());
      cart.getItems().add(event.getItem());
      return cart;
    }

    /** Changes the aggregate in place: the list the event was given. */
    @ApplyEvent
    public Cart apply(CheckedOut event, Cart state) {
      state.getItems().clear();
      return state;
    }
  }

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, Command> results;
  private TestOutputTopic<String, Event> events;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "mutable-aggregate-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(properties).registerHandler(new CartHandler()).build().topology());
    commands = driver.createInputTopic("commands.cart", new StringSerializer(), new JsonSerializer<>());
    results = driver.createOutputTopic("commands.cart.results", new StringDeserializer(), new JsonDeserializer<>(Command.class));
    events = driver.createOutputTopic("events.cart", new StringDeserializer(), new JsonDeserializer<>(Event.class));
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("Should store and send the event as the handler returned it, when applying it changes objects it shares with the aggregate")
  void applyingAnEventDoesNotChangeTheStoredEvent() {
    send(new AddItem("cart", "apple"));
    send(new AddItem("cart", "pear"));
    send(new CheckOut("cart"));

    Event stored = IteratorUtils.toList(driver.<String, Event>getKeyValueStore("event-store").all()).get(2).value;
    assertThat(((CheckedOut) stored.getPayload()).getItems()).containsExactly("apple", "pear");

    Event sent = events.readValuesToList().get(2);
    assertThat(((CheckedOut) sent.getPayload()).getItems()).containsExactly("apple", "pear");
  }

  @Test
  @DisplayName("Should answer a command without events as a success")
  void aCommandWithoutEventsSucceeds() {
    send(new AddItem("cart", "apple"));
    events.readValuesToList();
    results.readValuesToList();

    send(new Touch("cart"));

    Command result = results.readValue();
    assertThat(result.getType()).isEqualTo("Touch");
    assertThat(result.getMetadata().get(Metadata.RESULT)).isEqualTo("success");
    assertThat(events.isEmpty()).isTrue();
  }

  /** The correlation id is shared with the other commands of a flow; the causation id tells which command it was. */
  @Test
  @DisplayName("Should name the command that produced an event, and keep its correlation id")
  void anEventNamesItsCommand() {
    Command command = Command.builder().payload(new AddItem("cart", "apple")).metadata(Metadata.CORRELATION_ID, "saga").build();
    commands.pipeInput(command.getAggregateId(), command);

    Event event = events.readValue();
    assertThat(event.getMetadata().getCausationId()).isEqualTo(command.getId());
    assertThat(event.getMetadata().getCorrelationId()).isEqualTo("saga");
  }

  private void send(Object payload) {
    Command command = Command.builder().payload(payload).build();
    commands.pipeInput(command.getAggregateId(), command);
  }
}
