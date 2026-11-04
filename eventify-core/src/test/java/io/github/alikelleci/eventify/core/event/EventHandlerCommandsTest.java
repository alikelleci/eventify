package io.github.alikelleci.eventify.core.event;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.command.internal.HeaderNames;
import io.github.alikelleci.eventify.core.event.annotation.EventHandler;
import io.github.alikelleci.eventify.core.event.annotation.Priority;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.MetadataKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import lombok.Value;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** An event handler may return commands: Eventify sends them to their own topic, keyed by their own aggregate id. */
@DisplayName("Commands returned by event handlers")
class EventHandlerCommandsTest {

  @Topic("commands.order")
  public record PlaceOrder(@AggregateId String id, String paymentId, int amount) {
  }

  @Topic("events.order")
  public record OrderPlaced(@AggregateId String id, String paymentId, int amount) {
  }

  @AggregateRoot("order")
  public record Order(@AggregateId String id) {
  }

  @Topic("commands.payment")
  public record RequestPayment(@AggregateId String id, String orderId, int amount) {
  }

  @Topic("commands.payment")
  public record NotifyCustomer(@AggregateId String id) {
  }

  @Topic("events.payment")
  public record PaymentRequested(@AggregateId String id, String orderId, int amount) {
  }

  @AggregateRoot("payment")
  public record Payment(@AggregateId String id) {
  }

  public record NotACommand(@AggregateId String id) {
  }

  @Value
  @Topic("commands.payment")
  public static class CorruptPayment {
    @AggregateId
    String id;

    public String getContent() {
      throw new IllegalStateException("not writable");
    }
  }

  public static class OrderHandler {
    @CommandHandler
    public OrderPlaced handle(PlaceOrder command, Order state) {
      return new OrderPlaced(command.id(), command.paymentId(), command.amount());
    }

    @EventSourcingHandler
    public Order handle(OrderPlaced event, Order state) {
      return new Order(event.id());
    }
  }

  public static class PaymentHandler {
    @CommandHandler
    public PaymentRequested handle(RequestPayment command, Payment state) {
      return new PaymentRequested(command.id(), command.orderId(), command.amount());
    }

    @EventSourcingHandler
    public Payment handle(PaymentRequested event, Payment state) {
      return new Payment(event.id());
    }
  }

  public static class PaymentPolicy {
    @EventHandler
    public RequestPayment on(OrderPlaced event) {
      return new RequestPayment(event.paymentId(), event.id(), event.amount());
    }
  }

  public static class FirstPolicy {
    @EventHandler
    @Priority(1)
    public List<Object> on(OrderPlaced event) {
      return Arrays.asList(new RequestPayment(event.paymentId(), event.id(), event.amount()), null);
    }
  }

  public static class SecondPolicy {
    @EventHandler
    public NotifyCustomer on(OrderPlaced event) {
      return new NotifyCustomer(event.paymentId());
    }
  }

  public static class ProjectionsOnly {
    final List<String> seen = new ArrayList<>();

    @EventHandler
    public void project(OrderPlaced event) {
      seen.add(event.id());
    }

    @EventHandler
    public RequestPayment nothingToSend(OrderPlaced event) {
      return null;
    }
  }

  public static class BrokenPolicy {
    @EventHandler
    @Priority(1)
    public RequestPayment first(OrderPlaced event) {
      return new RequestPayment(event.paymentId(), event.id(), event.amount());
    }

    @EventHandler
    public Object second(OrderPlaced event) {
      return new NotACommand(event.id());
    }
  }

  public static class CorruptPolicy {
    @EventHandler
    public CorruptPayment on(OrderPlaced event) {
      return new CorruptPayment(event.paymentId());
    }
  }

  public static class UnaddressedPolicy {
    @EventHandler
    public RequestPayment on(OrderPlaced event) {
      return new RequestPayment(null, event.id(), event.amount());
    }
  }

  private TopologyTestDriver driver;

  @AfterEach
  void tearDown() {
    if (driver != null) {
      driver.close();
    }
  }

  @Test
  @DisplayName("Should send a returned command to its topic, keyed by its own aggregate id, with the event's correlation and the event as cause")
  void aReturnedCommandIsSent() {
    start(new PaymentPolicy());
    Event event = orderPlaced("order-1", "payment-1", Metadata.of(MetadataKeys.CORRELATION_ID, "flow-1").with("tenant", "acme"));

    pipe(event);

    TestRecord<String, Command> sent = paymentCommands().readRecord();
    assertThat(sent.key()).isEqualTo("payment-1");
    assertThat(sent.value().getPayload()).isEqualTo(new RequestPayment("payment-1", "order-1", 42));
    assertThat(sent.value().getMetadata())
        .containsEntry(MetadataKeys.CORRELATION_ID, "flow-1")
        .containsEntry(MetadataKeys.CAUSATION_ID, event.getId())
        .containsEntry("tenant", "acme");
    assertThat(sent.timestamp()).isEqualTo(sent.value().getTimestamp().toEpochMilli());
  }

  /** The event's record carries the reply topic of the command that produced it; the new command has its own sender. */
  @Test
  @DisplayName("Should not pass the event's reply topic on to the command")
  void theReplyTopicIsNotPassedOn() {
    start(new PaymentPolicy());

    Event event = orderPlaced("order-1", "payment-1", Metadata.EMPTY);
    driver.createInputTopic("events.order", new StringSerializer(), new EventSerde().serializer())
        .pipeInput(new TestRecord<>(event.getAggregateId(), event,
            new RecordHeaders().add(HeaderNames.REPLY_TO, "orders-api.replies".getBytes(StandardCharsets.UTF_8)), event.getTimestamp()));

    assertThat(paymentCommands().readRecord().headers().lastHeader(HeaderNames.REPLY_TO)).isNull();
  }

  @Test
  @DisplayName("Should send the commands of every handler, in priority order, and skip null entries")
  void commandsOfSeveralHandlersAreSentInPriorityOrder() {
    start(new SecondPolicy(), new FirstPolicy());

    pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY));

    assertThat(paymentCommands().readValuesToList()).extracting(Command::getPayload)
        .containsExactly(new RequestPayment("payment-1", "order-1", 42), new NotifyCustomer("payment-1"));
  }

  @Test
  @DisplayName("Should send nothing for a void handler or a returned null, and still run the handler")
  void voidAndNullSendNothing() {
    ProjectionsOnly projections = new ProjectionsOnly();
    start(projections);

    pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY));

    assertThat(projections.seen).containsExactly("order-1");
    assertThat(paymentCommands().isEmpty()).isTrue();
  }

  /** Sending the first and failing on the second would leave half a reaction: none is sent. */
  @Test
  @DisplayName("Should fail the event and send none of its commands when one returned value is not a command")
  void aNonCommandFailsTheEventAndSendsNothing() {
    start(new BrokenPolicy());

    assertThatThrownBy(() -> pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY)))
        .hasStackTraceContaining("NotACommand has no @Topic");
    assertThat(paymentCommands().isEmpty()).isTrue();
  }

  /** Its command handler couldn't read it and would skip it without a trace: it fails here instead. */
  @Test
  @DisplayName("Should fail the event and send nothing when a command cannot be written as JSON")
  void aCommandThatCannotBeWrittenAsJsonIsNotSent() {
    start(new CorruptPolicy());

    assertThatThrownBy(() -> pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY)))
        .hasStackTraceContaining("Command CorruptPayment cannot be written as JSON")
        .hasStackTraceContaining("not writable");
    assertThat(paymentCommands().isEmpty()).isTrue();
  }

  /** Without an id it has no key, so no partition and no aggregate to handle it. */
  @Test
  @DisplayName("Should fail the event and send nothing when a command has no aggregate id")
  void aCommandWithoutAnAggregateIdIsNotSent() {
    start(new UnaddressedPolicy());

    assertThatThrownBy(() -> pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY)))
        .hasStackTraceContaining("The @AggregateId field id of " + RequestPayment.class.getName() + " is null.");
    assertThat(paymentCommands().isEmpty()).isTrue();
  }

  /** In one instance, the command goes round to its own command handler. */
  @Test
  @DisplayName("Should let one aggregate's event lead to another aggregate's event")
  void anEventLeadsToAnotherAggregatesEvent() {
    start(new OrderHandler(), new PaymentHandler(), new PaymentPolicy());

    Command placeOrder = Command.builder().payload(new PlaceOrder("order-1", "payment-1", 42)).build();
    driver.createInputTopic("commands.order", new StringSerializer(), new CommandSerde().serializer())
        .pipeInput(placeOrder.getAggregateId(), placeOrder);

    Event paymentRequested = driver.createOutputTopic("events.payment", new StringDeserializer(), new EventSerde().deserializer())
        .readValue();
    assertThat(paymentRequested.getPayload()).isEqualTo(new PaymentRequested("payment-1", "order-1", 42));
    assertThat(paymentRequested.getMetadata().getCorrelationId()).isEqualTo(placeOrder.getMetadata().getCorrelationId());
  }

  private void start(Object... handlers) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-handler-commands-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    Eventify.EventifyBuilder builder = Eventify.builder().streamsConfig(properties);
    for (Object handler : handlers) {
      builder.registerHandler(handler);
    }
    driver = new TopologyTestDriver(builder.build().topology());
  }

  private static Event orderPlaced(String orderId, String paymentId, Metadata metadata) {
    return Event.builder().aggregateType("order").payload(new OrderPlaced(orderId, paymentId, 42)).metadata(metadata).sequence(1).build();
  }

  private void pipe(Event event) {
    driver.createInputTopic("events.order", new StringSerializer(), new EventSerde().serializer())
        .pipeInput(event.getAggregateId(), event);
  }

  private TestOutputTopic<String, Command> paymentCommands() {
    return driver.createOutputTopic("commands.payment", new StringDeserializer(), new CommandSerde().deserializer());
  }
}
