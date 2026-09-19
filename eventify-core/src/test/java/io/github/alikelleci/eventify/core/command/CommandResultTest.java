package io.github.alikelleci.eventify.core.command;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.message.MetadataKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
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

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** The result of a command, as its sender and the result topic get it, and the events it names. */
@DisplayName("Command result")
class CommandResultTest {

  private static final String REPLY_TOPIC = "carts-api.replies";

  @Topic("commands.cart")
  public interface CartCommand {
  }

  public record AddItem(@AggregateId String id, String item) implements CartCommand {
  }

  /** Accepted, without anything to change. */
  public record Touch(@AggregateId String id) implements CartCommand {
  }

  @Topic("events.cart")
  public record ItemAdded(@AggregateId String id, String item) {
  }

  @AggregateRoot
  public record Cart(@AggregateId String id, int items) {
  }

  public static class CartHandler {
    @HandleCommand
    public ItemAdded handle(AddItem command, Cart state) {
      return new ItemAdded(command.id(), command.item());
    }

    @HandleCommand
    public Object handle(Touch command, Cart state) {
      return null;
    }

    @ApplyEvent
    public Cart apply(ItemAdded event, Cart state) {
      return new Cart(event.id(), state == null ? 1 : state.items() + 1);
    }
  }

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, CommandResult> results;
  private TestOutputTopic<String, CommandResult> replies;
  private TestOutputTopic<String, Event> events;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "command-result-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(properties).registerHandler(new CartHandler()).build().topology());
    commands = driver.createInputTopic("commands.cart", new StringSerializer(), new CommandSerde().serializer());
    results = driver.createOutputTopic("commands.cart.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    replies = driver.createOutputTopic(REPLY_TOPIC, new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    events = driver.createOutputTopic("events.cart", new StringDeserializer(), new EventSerde().deserializer());
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("Should give the sender the events of the command, as they are stored and sent")
  void theSenderGetsTheEvents() {
    Command command = Command.builder().payload(new AddItem("cart-1", "apple")).build();
    command.getMetadata().put(MetadataKeys.REPLY_TO, REPLY_TOPIC);

    send(command);

    List<Event> sent = events.readValuesToList();
    CommandResult reply = replies.readValue();
    assertThat(sent).hasSize(1);
    assertThat(reply).isInstanceOfSatisfying(CommandResult.Success.class, success -> {
      assertThat(success.command().getId()).isEqualTo(command.getId());
      assertThat(success.events()).extracting(Event::getId).containsExactly(sent.get(0).getId());
      assertThat(success.events().get(0).getPayload()).isEqualTo(sent.get(0).getPayload());
      assertThat(success.events().get(0).getMetadata()).doesNotContainKey(MetadataKeys.REPLY_TO);
    });
    // Where to reply to is for the command's sender only: the event keeps the rest of the command's metadata.
    assertThat(sent.get(0).getMetadata())
        .doesNotContainKey(MetadataKeys.REPLY_TO)
        .containsEntry(MetadataKeys.CORRELATION_ID, command.getMetadata().getCorrelationId());
    assertThat(results.readValue()).isEqualTo(reply);
  }

  @Test
  @DisplayName("Should not send a result to a reply topic when the sender doesn't wait for it")
  void noReplyWithoutAReplyTopic() {
    send(Command.builder().payload(new AddItem("cart-1", "apple")).build());

    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
    assertThat(replies.isEmpty()).isTrue();
  }

  @Test
  @DisplayName("Should answer a command without events as a success")
  void aCommandWithoutEventsSucceeds() {
    send(Command.builder().payload(new Touch("cart-1")).build());

    CommandResult result = results.readValue();
    assertThat(result.command().getType()).isEqualTo("Touch");
    assertThat(result).isInstanceOfSatisfying(CommandResult.Success.class, success -> assertThat(success.events()).isEmpty());
    assertThat(events.isEmpty()).isTrue();
  }

  /** The correlation id is shared with the other commands of a flow; the causation id tells which command it was. */
  @Test
  @DisplayName("Should name the command that produced an event, and keep its correlation id")
  void anEventNamesItsCommand() {
    Command command = Command.builder().payload(new AddItem("cart-1", "apple")).metadata(MetadataKeys.CORRELATION_ID, "saga").build();

    send(command);

    Event event = events.readValue();
    assertThat(event.getMetadata().getCausationId()).isEqualTo(command.getId());
    assertThat(event.getMetadata().getCorrelationId()).isEqualTo("saga");
  }

  private void send(Command command) {
    commands.pipeInput(command.getAggregateId(), command);
  }
}
