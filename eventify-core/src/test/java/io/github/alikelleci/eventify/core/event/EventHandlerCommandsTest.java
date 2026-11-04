package io.github.alikelleci.eventify.core.event;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.Scheduled;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.command.internal.HeaderNames;
import io.github.alikelleci.eventify.core.event.annotation.EventHandler;
import io.github.alikelleci.eventify.core.event.annotation.Priority;
import io.github.alikelleci.eventify.core.event.exception.EventHandlingException;
import io.github.alikelleci.eventify.core.handler.annotation.Timestamp;
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
import java.time.Duration;
import java.time.Instant;
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

  public static class PaymentEventHandler {
    @EventHandler
    public RequestPayment on(OrderPlaced event) {
      return new RequestPayment(event.paymentId(), event.id(), event.amount());
    }
  }

  public static class FirstEventHandler {
    @EventHandler
    @Priority(1)
    public List<Object> on(OrderPlaced event) {
      return Arrays.asList(new RequestPayment(event.paymentId(), event.id(), event.amount()), null);
    }
  }

  public static class SecondEventHandler {
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

  public static class BrokenEventHandler {
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

  public static class CorruptEventHandler {
    @EventHandler
    public CorruptPayment on(OrderPlaced event) {
      return new CorruptPayment(event.paymentId());
    }
  }

  public static class UnaddressedEventHandler {
    @EventHandler
    public RequestPayment on(OrderPlaced event) {
      return new RequestPayment(null, event.id(), event.amount());
    }
  }

  /** Scheduled half an hour after the event, by the event's own time. */
  public static class SchedulingEventHandler {
    @EventHandler
    public Scheduled on(OrderPlaced event, @Timestamp Instant placedAt) {
      return Scheduled.of(new RequestPayment(event.paymentId(), event.id(), event.amount()), placedAt.plus(Duration.ofMinutes(30)));
    }
  }

  /** Adds its own keys, replaces one of the event's, and tries to change the flow and the cause. */
  public static class EnvelopeEventHandler {
    @EventHandler
    public Command on(OrderPlaced event) {
      return Command.builder()
          .payload(new RequestPayment(event.paymentId(), event.id(), event.amount()))
          .metadata(Metadata.of("priority", "high").with("tenant", "beta")
              .with(MetadataKeys.CORRELATION_ID, "another-flow").with(MetadataKeys.CAUSATION_ID, "another-cause"))
          .build();
    }
  }

  /** Every kind of value in one list: a payload, a command, a scheduled payload, a scheduled command and a null. */
  public static class MixingEventHandler {
    static final Duration LATER = Duration.ofMinutes(10);

    @EventHandler
    public List<Object> on(OrderPlaced event, @Timestamp Instant placedAt) {
      return Arrays.asList(
          new RequestPayment(event.paymentId(), event.id(), event.amount()),
          Command.builder().payload(new NotifyCustomer("notify-" + event.paymentId())).metadata(Metadata.of("channel", "email")).build(),
          null,
          Scheduled.of(new RequestPayment("later-" + event.paymentId(), event.id(), event.amount()), placedAt.plus(LATER)),
          Scheduled.of(Command.builder().payload(new NotifyCustomer("reminder-" + event.paymentId())).metadata(Metadata.of("reason", "unpaid")).build(),
              placedAt.plus(LATER.multipliedBy(2))));
    }
  }

  /** Its scheduled command has no topic: none of the others may be sent. */
  public static class BrokenSchedulingEventHandler {
    @EventHandler
    public List<Object> on(OrderPlaced event, @Timestamp Instant placedAt) {
      return List.of(
          new RequestPayment(event.paymentId(), event.id(), event.amount()),
          Scheduled.of(new RequestPayment("later-" + event.paymentId(), event.id(), event.amount()), placedAt),
          Scheduled.of(new NotACommand(event.id()), placedAt));
    }
  }

  /** Fails while building its result: the command before it is not sent. */
  public static class TimelessSchedulingEventHandler {
    @EventHandler
    public List<Object> on(OrderPlaced event) {
      return List.of(new RequestPayment(event.paymentId(), event.id(), event.amount()), Scheduled.of(new NotifyCustomer(event.paymentId()), null));
    }
  }

  /** A command without an aggregate id, wrapped: it has no key. */
  public static class UnaddressedEnvelopeEventHandler {
    @EventHandler
    public Command on(OrderPlaced event) {
      return Command.builder().payload(new RequestPayment(null, event.id(), event.amount())).build();
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
    start(new PaymentEventHandler());
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
    start(new PaymentEventHandler());

    Event event = orderPlaced("order-1", "payment-1", Metadata.EMPTY);
    driver.createInputTopic("events.order", new StringSerializer(), new EventSerde().serializer())
        .pipeInput(new TestRecord<>(event.getAggregateId(), event,
            new RecordHeaders().add(HeaderNames.REPLY_TO, "orders-api.replies".getBytes(StandardCharsets.UTF_8)), event.getTimestamp()));

    assertThat(paymentCommands().readRecord().headers().lastHeader(HeaderNames.REPLY_TO)).isNull();
  }

  @Test
  @DisplayName("Should send the commands of every handler, in priority order, and skip null entries")
  void commandsOfSeveralHandlersAreSentInPriorityOrder() {
    start(new SecondEventHandler(), new FirstEventHandler());

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
    start(new BrokenEventHandler());

    assertThatThrownBy(() -> pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY)))
        .hasStackTraceContaining(EventHandlingException.class.getName())
        .hasStackTraceContaining("NotACommand has no @Topic");
    assertThat(paymentCommands().isEmpty()).isTrue();
  }

  /** Its command handler couldn't read it and would skip it without a trace: it fails here instead. */
  @Test
  @DisplayName("Should fail the event and send nothing when a command cannot be written as JSON")
  void aCommandThatCannotBeWrittenAsJsonIsNotSent() {
    start(new CorruptEventHandler());

    assertThatThrownBy(() -> pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY)))
        .hasStackTraceContaining("Command CorruptPayment cannot be written as JSON")
        .hasStackTraceContaining("not writable");
    assertThat(paymentCommands().isEmpty()).isTrue();
  }

  /** Without an id it has no key, so no partition and no aggregate to handle it. */
  @Test
  @DisplayName("Should fail the event and send nothing when a command has no aggregate id")
  void aCommandWithoutAnAggregateIdIsNotSent() {
    start(new UnaddressedEventHandler());

    assertThatThrownBy(() -> pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY)))
        .hasStackTraceContaining("The @AggregateId field id of " + RequestPayment.class.getName() + " is null.");
    assertThat(paymentCommands().isEmpty()).isTrue();
  }

  /** In one instance, the command goes round to its own command handler. */
  @Test
  @DisplayName("Should let one aggregate's event lead to another aggregate's event")
  void anEventLeadsToAnotherAggregatesEvent() {
    start(new OrderHandler(), new PaymentHandler(), new PaymentEventHandler());

    Command placeOrder = Command.builder().payload(new PlaceOrder("order-1", "payment-1", 42)).build();
    driver.createInputTopic("commands.order", new StringSerializer(), new CommandSerde().serializer())
        .pipeInput(placeOrder.getAggregateId(), placeOrder);

    Event paymentRequested = driver.createOutputTopic("events.payment", new StringDeserializer(), new EventSerde().deserializer())
        .readValue();
    assertThat(paymentRequested.getPayload()).isEqualTo(new PaymentRequested("payment-1", "order-1", 42));
    assertThat(paymentRequested.getMetadata().getCorrelationId()).isEqualTo(placeOrder.getMetadata().getCorrelationId());
  }

  @Test
  @DisplayName("Should send a scheduled command to its scheduled topic with its delivery time, not to its command topic")
  void aScheduledCommandGoesToTheScheduledTopic() {
    start(new SchedulingEventHandler());
    Event event = orderPlaced("order-1", "payment-1", Metadata.of(MetadataKeys.CORRELATION_ID, "flow-1"));

    pipe(event);

    assertThat(paymentCommands().isEmpty()).as("sent right away").isTrue();
    TestRecord<String, Command> scheduled = scheduledPaymentCommands().readRecord();
    assertThat(scheduled.key()).isEqualTo("payment-1");
    assertThat(scheduled.value().getPayload()).isEqualTo(new RequestPayment("payment-1", "order-1", 42));
    assertThat(deliverAt(scheduled)).isEqualTo(event.getTimestamp().plus(Duration.ofMinutes(30)).toEpochMilli());
    assertThat(scheduled.headers().lastHeader(HeaderNames.REPLY_TO)).isNull();
    assertThat(scheduled.value().getMetadata())
        .containsEntry(MetadataKeys.CORRELATION_ID, "flow-1")
        .containsEntry(MetadataKeys.CAUSATION_ID, event.getId());
  }

  /** In one instance: the scheduler keeps it, delivers it at its time, and the payment aggregate handles it. */
  @Test
  @DisplayName("Should deliver a command scheduled by an event handler at its time, and have it handled in the same flow")
  void aScheduledCommandIsHandledAtItsTime() {
    Instant now = Instant.parse("2026-11-04T12:00:00Z");
    start(now, new PaymentHandler(), new SchedulingEventHandler());
    Event event = Event.builder().aggregateType("order").payload(new OrderPlaced("order-1", "payment-1", 42))
        .metadata(Metadata.of(MetadataKeys.CORRELATION_ID, "flow-1")).sequence(1).build();

    pipe(event);
    TestOutputTopic<String, Event> paymentEvents = driver.createOutputTopic("events.payment", new StringDeserializer(), new EventSerde().deserializer());
    driver.advanceWallClockTime(Duration.between(now, event.getTimestamp().plus(Duration.ofMinutes(29))));
    assertThat(paymentEvents.isEmpty()).as("handled before its time").isTrue();

    driver.advanceWallClockTime(Duration.ofMinutes(2));
    Event paymentRequested = paymentEvents.readValue();
    assertThat(paymentRequested.getPayload()).isEqualTo(new PaymentRequested("payment-1", "order-1", 42));
    assertThat(paymentRequested.getMetadata().getCorrelationId()).isEqualTo("flow-1");
    assertThat(paymentEvents.isEmpty()).as("handled twice").isTrue();
  }

  @Test
  @DisplayName("Should add and replace the handler's own metadata keys, and keep the event's correlation and cause")
  void aCommandEnvelopeAddsItsMetadata() {
    start(new EnvelopeEventHandler());
    Event event = orderPlaced("order-1", "payment-1", Metadata.of(MetadataKeys.CORRELATION_ID, "flow-1").with("tenant", "acme").with("user", "ada"));

    pipe(event);

    Command sent = paymentCommands().readValue();
    assertThat(sent.getPayload()).isEqualTo(new RequestPayment("payment-1", "order-1", 42));
    assertThat(sent.getMetadata())
        .containsEntry("priority", "high")
        .containsEntry("tenant", "beta")
        .containsEntry("user", "ada")
        .containsEntry(MetadataKeys.CORRELATION_ID, "flow-1")
        .containsEntry(MetadataKeys.CAUSATION_ID, event.getId());
  }

  @Test
  @DisplayName("Should send every value of a mixed list in order, each to its topic with its own metadata, and skip nulls")
  void aMixedListIsSentInOrder() {
    start(new MixingEventHandler());
    Event event = orderPlaced("order-1", "payment-1", Metadata.of(MetadataKeys.CORRELATION_ID, "flow-1").with("tenant", "acme"));

    pipe(event);

    List<TestRecord<String, Command>> now = paymentCommands().readRecordsToList();
    assertThat(now).extracting(record -> record.value().getPayload())
        .containsExactly(new RequestPayment("payment-1", "order-1", 42), new NotifyCustomer("notify-payment-1"));
    assertThat(now).extracting(TestRecord::key).containsExactly("payment-1", "notify-payment-1");
    assertThat(now.get(0).value().getMetadata()).doesNotContainKey("channel");
    assertThat(now.get(1).value().getMetadata()).containsEntry("channel", "email");

    List<TestRecord<String, Command>> later = scheduledPaymentCommands().readRecordsToList();
    assertThat(later).extracting(record -> record.value().getPayload())
        .containsExactly(new RequestPayment("later-payment-1", "order-1", 42), new NotifyCustomer("reminder-payment-1"));
    assertThat(later).extracting(this::deliverAt).containsExactly(
        event.getTimestamp().plus(MixingEventHandler.LATER).toEpochMilli(),
        event.getTimestamp().plus(MixingEventHandler.LATER.multipliedBy(2)).toEpochMilli());
    assertThat(later.get(1).value().getMetadata()).containsEntry("reason", "unpaid");

    assertThat(now).noneMatch(record -> record.headers().lastHeader(HeaderNames.DELIVER_AT) != null);
    assertThat(List.of(now, later)).allSatisfy(records -> assertThat(records).allSatisfy(record -> assertThat(record.value().getMetadata())
        .containsEntry("tenant", "acme")
        .containsEntry(MetadataKeys.CORRELATION_ID, "flow-1")
        .containsEntry(MetadataKeys.CAUSATION_ID, event.getId())));
  }

  /** Sending the others and failing on one would leave half a reaction: none is sent, now or later. */
  @Test
  @DisplayName("Should fail the event and send nothing, now or later, when one scheduled command has no topic")
  void aBrokenScheduledCommandSendsNothing() {
    start(new BrokenSchedulingEventHandler());

    assertThatThrownBy(() -> pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY)))
        .hasStackTraceContaining("NotACommand has no @Topic");
    assertThat(paymentCommands().isEmpty()).isTrue();
    assertThat(scheduledPaymentCommands().isEmpty()).isTrue();
  }

  @Test
  @DisplayName("Should fail the event and send nothing when a scheduled command has no delivery time")
  void aScheduledCommandWithoutATimeSendsNothing() {
    start(new TimelessSchedulingEventHandler());

    assertThatThrownBy(() -> pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY)))
        .hasStackTraceContaining("A scheduled command needs a command and a delivery time.");
    assertThat(paymentCommands().isEmpty()).isTrue();
    assertThat(scheduledPaymentCommands().isEmpty()).isTrue();
  }

  @Test
  @DisplayName("Should fail the event and send nothing when a returned command envelope has no aggregate id")
  void anEnvelopeWithoutAnAggregateIdIsNotSent() {
    start(new UnaddressedEnvelopeEventHandler());

    assertThatThrownBy(() -> pipe(orderPlaced("order-1", "payment-1", Metadata.EMPTY)))
        .hasStackTraceContaining(EventHandlingException.class.getName())
        .hasStackTraceContaining("The @AggregateId field id of " + RequestPayment.class.getName() + " is null.");
    assertThat(paymentCommands().isEmpty()).isTrue();
  }

  private long deliverAt(TestRecord<String, Command> record) {
    return Long.parseLong(new String(record.headers().lastHeader(HeaderNames.DELIVER_AT).value(), StandardCharsets.UTF_8));
  }

  private TestOutputTopic<String, Command> scheduledPaymentCommands() {
    return driver.createOutputTopic("commands.payment.scheduled", new StringDeserializer(), new CommandSerde().deserializer());
  }

  private void start(Instant wallClock, Object... handlers) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-handler-commands-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    Eventify.EventifyBuilder builder = Eventify.builder().streamsConfig(properties);
    for (Object handler : handlers) {
      builder.registerHandler(handler);
    }
    driver = new TopologyTestDriver(builder.build().topology(), properties, wallClock);
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
