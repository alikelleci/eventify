package io.github.alikelleci.eventify.core.command;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.kafka.HeaderNames;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.MetadataKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
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

  public record AddItemForUser(@AggregateId String id, String item, String user) implements CartCommand {
  }

  @Topic("events.cart")
  public record ItemAdded(@AggregateId String id, String item) {
  }

  @AggregateRoot("cart")
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

    /** Works out metadata of its own; what it makes is its own copy, and the command keeps the metadata it came with. */
    @HandleCommand
    public ItemAdded handle(AddItemForUser command, Cart state, Metadata metadata) {
      Metadata mine = metadata.with("user", command.user());
      return new ItemAdded(command.id(), command.item() + " for " + mine.get("user"));
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
    CommandResultSerde resultSerde = new CommandResultSerde(EventifyObjectMapper.create());
    results = driver.createOutputTopic("commands.cart.results", new StringDeserializer(), resultSerde.deserializer());
    replies = driver.createOutputTopic(REPLY_TOPIC, new StringDeserializer(), resultSerde.deserializer());
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

    sendAwaitingReply(command);

    List<Event> sent = events.readValuesToList();
    CommandResult reply = replies.readValue();
    assertThat(sent).hasSize(1);
    assertThat(reply).isInstanceOfSatisfying(CommandResult.Success.class, success -> {
      assertThat(success.command().getId()).isEqualTo(command.getId());
      assertThat(success.events()).extracting(Event::getId).containsExactly(sent.get(0).getId());
      assertThat(success.events().get(0).getPayload()).isEqualTo(sent.get(0).getPayload());
    });
    // An event takes over its command's metadata, and names the command as its cause. Where to reply to is not in it:
    // it travels as a header on the command record, and is only for the command's sender.
    assertThat(sent.get(0).getMetadata())
        .containsOnlyKeys(MetadataKeys.CORRELATION_ID, MetadataKeys.CAUSATION_ID)
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

  @Test
  @DisplayName("Should number the events of an aggregate in the order they were handled, whatever the clocks of their senders")
  void eventsAreNumberedInTheOrderTheyWereHandled() {
    Instant now = Instant.now();
    send(Command.builder().payload(new AddItem("cart-1", "apple")).build(), now);
    send(Command.builder().payload(new AddItem("cart-2", "pear")).build(), now);
    send(Command.builder().payload(new AddItem("cart-1", "bread")).build(), now.minusSeconds(60)); // a clock behind

    assertThat(events.readValuesToList())
        .extracting(event -> event.getAggregateId() + " " + event.getSequence())
        .containsExactly("cart-1 1", "cart-2 1", "cart-1 2");
  }

  /** E.g. a store edited by hand: the other aggregates go on, this one is refused with the reason until it is fixed. */
  @Test
  @DisplayName("Should reject the commands of an aggregate whose stored events have a gap, and say why")
  void anAggregateWithAGapIsRejected() {
    KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
    eventStore.put(StoreKeys.of("cart", "cart-1", 1), Event.builder().aggregateType("cart").payload(new ItemAdded("cart-1", "apple")).sequence(1).build());
    eventStore.put(StoreKeys.of("cart", "cart-1", 3), Event.builder().aggregateType("cart").payload(new ItemAdded("cart-1", "pear")).sequence(3).build());

    send(Command.builder().payload(new AddItem("cart-1", "bread")).build());
    send(Command.builder().payload(new AddItem("cart-2", "bread")).build());

    List<CommandResult> results = this.results.readValuesToList();
    assertThat(results.get(0)).isInstanceOfSatisfying(CommandResult.Failure.class,
        failure -> assertThat(failure.cause()).contains("expected #2, found #3"));
    assertThat(results.get(1)).isInstanceOf(CommandResult.Success.class);
    assertThat(events.readValuesToList()).extracting(Event::getAggregateId).containsExactly("cart-2");
  }

  /** The correlation id is shared with the other commands of a flow; the causation id tells which command it was. */
  @Test
  @DisplayName("Should name the command that produced an event, and keep its correlation id")
  void anEventNamesItsCommand() {
    Command command = Command.builder().payload(new AddItem("cart-1", "apple")).metadata(Metadata.of(MetadataKeys.CORRELATION_ID, "saga")).build();

    send(command);

    Event event = events.readValue();
    assertThat(event.getMetadata().getCausationId()).isEqualTo(command.getId());
    assertThat(event.getMetadata().getCorrelationId()).isEqualTo("saga");
  }

  /** What a handler adds is for the handler: the events carry the metadata of the command, and nothing else. */
  @Test
  @DisplayName("Should not carry metadata a handler made for itself into the events")
  void metadataAHandlerMakesStaysWithTheHandler() {
    Command command = Command.builder().payload(new AddItemForUser("cart-1", "apple", "ada")).build();

    send(command);

    Event event = events.readValue();
    assertThat(event.getPayload()).isEqualTo(new ItemAdded("cart-1", "apple for ada")); // the handler did use it
    assertThat(event.getMetadata()).containsOnlyKeys(MetadataKeys.CORRELATION_ID, MetadataKeys.CAUSATION_ID);
    assertThat(command.getMetadata()).containsOnlyKeys(MetadataKeys.CORRELATION_ID);
  }

  private void send(Command command) {
    commands.pipeInput(command.getAggregateId(), command);
  }

  /** Sends the command as a host whose clock says this: the timestamp of its record. */
  private void send(Command command, Instant recordTime) {
    commands.pipeInput(command.getAggregateId(), command, recordTime);
  }

  /** Sends the command the way a sender that waits for its result does: with the reply topic as a record header. */
  private void sendAwaitingReply(Command command) {
    commands.pipeInput(new TestRecord<>(command.getAggregateId(), command,
        new RecordHeaders().add(HeaderNames.REPLY_TO, REPLY_TOPIC.getBytes(StandardCharsets.UTF_8)), command.getTimestamp()));
  }
}
