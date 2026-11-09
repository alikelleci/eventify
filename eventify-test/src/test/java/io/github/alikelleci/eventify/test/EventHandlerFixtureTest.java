package io.github.alikelleci.eventify.test;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.Scheduled;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.annotation.EventHandler;
import io.github.alikelleci.eventify.core.handler.annotation.Timestamp;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMissingException;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;
import static io.github.alikelleci.eventify.test.EventHandlerFixture.after;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Event handler fixture")
class EventHandlerFixtureTest {

  static final Duration PAYMENT_TIME = Duration.ofMinutes(30);
  static final Duration REMINDER_TIME = Duration.ofDays(1);

  @Topic("events.order")
  public record OrderPlaced(@AggregateId String id, String customer) {
  }

  @Topic("events.order")
  public record OrderPaid(@AggregateId String id, String customer) {
  }

  /** Reserved for a day: two scheduled commands. */
  @Topic("events.order")
  public record OrderReserved(@AggregateId String id, String customer) {
  }

  /** Two parcels: two commands for one topic. */
  @Topic("events.order")
  public record OrderSplit(@AggregateId String id) {
  }

  @Topic("events.order")
  public record OrderCancelled(@AggregateId String id) {
  }

  @Topic("events.order")
  public record OrderLost(@AggregateId String id) {
  }

  /** Its handler forgets the order's id in the command. */
  @Topic("events.order")
  public record OrderMislaid(@AggregateId String id) {
  }

  /** Has no event handler. */
  @Topic("events.order")
  public record OrderArchived(@AggregateId String id) {
  }

  @Topic("commands.payment")
  public record RequestPayment(@AggregateId String orderId) {
  }

  @Topic("commands.order")
  public record CancelUnpaidOrder(@AggregateId String id) {
  }

  @Topic("commands.shipment")
  public record ShipParcel(@AggregateId String orderId, int parcel) {
  }

  @Topic("commands.customer")
  public record NotifyCustomer(@AggregateId String customer, String reason) {
  }

  public static class OrderHandler {
    @EventHandler
    public List<Object> handle(OrderPlaced event, @Timestamp Instant placedAt) {
      return List.of(new RequestPayment(event.id()), Scheduled.of(new CancelUnpaidOrder(event.id()), placedAt.plus(PAYMENT_TIME)));
    }

    @EventHandler
    public List<Object> handle(OrderPaid event) {
      return List.of(new ShipParcel(event.id(), 1),
          Command.builder().payload(new NotifyCustomer(event.customer(), "paid")).metadata(Metadata.of("channel", "mail")).build());
    }

    @EventHandler
    public List<Object> handle(OrderReserved event, @Timestamp Instant reservedAt) {
      return List.of(Scheduled.of(new NotifyCustomer(event.customer(), "reminder"), reservedAt.plus(REMINDER_TIME)),
          Scheduled.of(new CancelUnpaidOrder(event.id()), reservedAt.plus(PAYMENT_TIME)));
    }

    @EventHandler
    public List<Object> handle(OrderSplit event) {
      return List.of(new ShipParcel(event.id(), 1), new ShipParcel(event.id(), 2));
    }

    @EventHandler
    public Object handle(OrderCancelled event) {
      return null;
    }

    @EventHandler
    public Object handle(OrderMislaid event) {
      return new ShipParcel(null, 1);
    }

    @EventHandler
    public Object handle(OrderLost event) {
      throw new IllegalStateException("Order " + event.id() + " is lost.");
    }
  }

  @AggregateRoot("order")
  public record Order(@AggregateId String id) {
  }

  public static class OrderCommands {
    @CommandHandler
    public Object handle(CancelUnpaidOrder command, Order state) {
      return null;
    }
  }

  private final EventHandlerFixture fixture = EventHandlerFixture.of(new OrderHandler());

  @Test
  @DisplayName("Should give the commands sent now and the scheduled ones, timed from the event")
  void commandsAndScheduled() {
    EventHandlerFixture.Then then = fixture.when(new OrderPlaced("order-1", "ada"))
        .expectCommands(new RequestPayment("order-1"))
        .expectScheduled(after(PAYMENT_TIME, new CancelUnpaidOrder("order-1")));

    then.expectScheduled(Scheduled.of(new CancelUnpaidOrder("order-1"), then.event().getTimestamp().plus(PAYMENT_TIME)));
    assertThat(then.commands()).containsExactly(new RequestPayment("order-1"));
    assertThat(then.scheduled()).extracting(Scheduled::command).containsExactly(new CancelUnpaidOrder("order-1"));
  }

  @Test
  @DisplayName("Should expect several scheduled commands in one call, timed after the event or at a time")
  void severalScheduled() {
    EventHandlerFixture.Then then = fixture.when(new OrderReserved("order-1", "ada"))
        .expectCommands()
        .expectScheduled(after(PAYMENT_TIME, new CancelUnpaidOrder("order-1")), after(REMINDER_TIME, new NotifyCustomer("ada", "reminder")))
        .expectScheduled(after(REMINDER_TIME, new NotifyCustomer("ada", "reminder")), after(PAYMENT_TIME, new CancelUnpaidOrder("order-1")));

    Instant reservedAt = then.event().getTimestamp();
    then.expectScheduled(Scheduled.of(new CancelUnpaidOrder("order-1"), reservedAt.plus(PAYMENT_TIME)),
        after(REMINDER_TIME, new NotifyCustomer("ada", "reminder")));
    assertThatThrownBy(() -> then.expectScheduled(after(PAYMENT_TIME, new CancelUnpaidOrder("order-1"))))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  @DisplayName("Should fail when a scheduled command comes at another time after the event")
  void otherDelay() {
    EventHandlerFixture.Then then = fixture.when(new OrderPlaced("order-1", "ada"));
    Instant expected = then.event().getTimestamp().plus(Duration.ofMinutes(31));

    assertThatThrownBy(() -> then.expectScheduled(after(Duration.ofMinutes(31), new CancelUnpaidOrder("order-1"))))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("to schedule [Scheduled[command=CancelUnpaidOrder[id=order-1], deliverAt=" + expected.truncatedTo(ChronoUnit.MILLIS) + "]]");
  }

  @Test
  @DisplayName("Should refuse what isn't a scheduled command, and a negative delay")
  void scheduledRefusals() {
    EventHandlerFixture.Then then = fixture.when(new OrderPlaced("order-1", "ada"));

    assertThatThrownBy(() -> then.expectScheduled(new CancelUnpaidOrder("order-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("expectScheduled takes after(delay, command) or Scheduled.of(command, time), not CancelUnpaidOrder.");
    assertThatThrownBy(() -> after(Duration.ofMinutes(-1), new CancelUnpaidOrder("order-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("A delay can't be negative, not PT-1M.");
    assertThatThrownBy(() -> after(null, new CancelUnpaidOrder("order-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("A scheduled command needs a delay and a command.");
  }

  @Test
  @DisplayName("Should check the order per topic only, as Kafka keeps it")
  void orderPerTopic() {
    fixture.when(new OrderPaid("order-1", "ada"))
        .expectCommands(new NotifyCustomer("ada", "paid"), new ShipParcel("order-1", 1))
        .expectCommands(new ShipParcel("order-1", 1), new NotifyCustomer("ada", "paid"));

    EventHandlerFixture.Then split = fixture.when(new OrderSplit("order-1"))
        .expectCommands(new ShipParcel("order-1", 1), new ShipParcel("order-1", 2));
    assertThatThrownBy(() -> split.expectCommands(new ShipParcel("order-1", 2), new ShipParcel("order-1", 1)))
        .isInstanceOf(AssertionError.class)
        .hasMessage("Expected OrderSplit to send commands [ShipParcel[orderId=order-1, parcel=2], ShipParcel[orderId=order-1, parcel=1]],"
            + " but it sent [ShipParcel[orderId=order-1, parcel=1], ShipParcel[orderId=order-1, parcel=2]].");
  }

  @Test
  @DisplayName("Should fail when the commands or the scheduled ones differ")
  void otherCommands() {
    EventHandlerFixture.Then then = fixture.when(new OrderPlaced("order-1", "ada"));
    Instant deliverAt = then.event().getTimestamp().plus(PAYMENT_TIME);

    assertThatThrownBy(() -> then.expectCommands(new RequestPayment("order-2"))).isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> then.expectCommands()).isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> then.expectScheduled(Scheduled.of(new CancelUnpaidOrder("order-1"), deliverAt.plusSeconds(1))))
        .isInstanceOf(AssertionError.class)
        .hasMessageStartingWith("Expected OrderPlaced to schedule [Scheduled[command=CancelUnpaidOrder[id=order-1]");
    assertThatThrownBy(then::expectNoCommands)
        .isInstanceOf(AssertionError.class)
        .hasMessageStartingWith("Expected OrderPlaced to send no commands, but it sent [RequestPayment[orderId=order-1]] and scheduled");
    then.expectScheduled(Scheduled.of(new CancelUnpaidOrder("order-1"), deliverAt));
  }

  /** Its metadata wouldn't be compared: sent() has it. */
  @Test
  @DisplayName("Should refuse a Command as an expected command")
  void commandAsExpectation() {
    EventHandlerFixture.Then then = fixture.when(new OrderPlaced("order-1", "ada"));
    Command payment = Command.builder().payload(new RequestPayment("order-1")).metadata(Metadata.of("channel", "mail")).build();
    Command cancel = Command.builder().payload(new CancelUnpaidOrder("order-1")).build();

    assertThatThrownBy(() -> then.expectCommands(payment))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("expectCommands takes the payloads of commands, not a Command: check its metadata with sent().");
    assertThatThrownBy(() -> then.expectScheduled(after(PAYMENT_TIME, cancel)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("expectScheduled takes the payloads of commands, not a Command: check its metadata with sent().");
    assertThatThrownBy(() -> then.expectScheduled(Scheduled.of(cancel, then.event().getTimestamp().plus(PAYMENT_TIME))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("Should send no commands when the handler returns none")
  void noCommands() {
    fixture.when(new OrderCancelled("order-1"))
        .expectSuccess()
        .expectNoCommands()
        .expectCommands()
        .expectScheduled();
  }

  @Test
  @DisplayName("Should give the failure of a handler, and send none of its commands")
  void failure() {
    EventHandlerFixture.Then then = fixture.when(new OrderLost("order-1"))
        .expectFailure()
        .expectFailure(IllegalStateException.class)
        .expectFailure(IllegalStateException.class, "Order order-1 is lost.");

    assertThat(then.cause()).isEqualTo("IllegalStateException: Order order-1 is lost.");
    assertThat(then.sent()).isEmpty();
    assertThatThrownBy(then::expectSuccess)
        .isInstanceOf(AssertionError.class)
        .hasMessage("Expected OrderLost to be handled, but it failed: IllegalStateException: Order order-1 is lost.");
    assertThatThrownBy(then::expectNoCommands).isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> then.expectFailure(IllegalArgumentException.class))
        .isInstanceOf(AssertionError.class)
        .hasMessage("Expected OrderLost to fail with IllegalArgumentException, but it failed with: IllegalStateException: Order order-1 is lost.");
    assertThatThrownBy(() -> then.expectFailure(IllegalStateException.class, "Order order-2 is lost."))
        .isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> fixture.when(new OrderCancelled("order-1")).expectFailure())
        .isInstanceOf(AssertionError.class)
        .hasMessage("Expected OrderCancelled to fail, but it was handled and sent [].");
  }

  @Test
  @DisplayName("Should send commands with the event's metadata and the handler's own")
  void metadata() {
    Event paid = Event.builder().aggregateType("order").sequence(2)
        .payload(new OrderPaid("order-1", "ada")).metadata(Metadata.of("user", "bob")).build();

    List<Command> sent = fixture.when(paid).sent();

    assertThat(sent).extracting(Command::getPayload).containsExactlyInAnyOrder(new ShipParcel("order-1", 1), new NotifyCustomer("ada", "paid"));
    assertThat(sent).allSatisfy(command -> {
      assertThat(command.getMetadata().get("user")).isEqualTo("bob");
      assertThat(command.getMetadata().get(CAUSATION_ID)).isEqualTo(paid.getId());
    });
    assertThat(sent).filteredOn(command -> command.getPayload() instanceof NotifyCustomer)
        .singleElement().satisfies(command -> assertThat(command.getMetadata().get("channel")).isEqualTo("mail"));
  }

  @Test
  @DisplayName("Should give the scheduled commands as sent, after the ones sent now")
  void sentScheduled() {
    List<Command> sent = fixture.when(new OrderPlaced("order-1", "ada")).sent();

    assertThat(sent).extracting(Command::getPayload).containsExactly(new RequestPayment("order-1"), new CancelUnpaidOrder("order-1"));
  }

  @Test
  @DisplayName("Should use the application's ObjectMapper")
  void applicationObjectMapper() {
    fixture.withObjectMapper(EventifyObjectMapper.create())
        .when(new OrderSplit("order-1"))
        .expectCommands(new ShipParcel("order-1", 1), new ShipParcel("order-1", 2));
  }

  @Test
  @DisplayName("Should refuse an event without a handler, and command handlers")
  void refusals() {
    assertThatThrownBy(() -> fixture.when(new OrderArchived("order-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No event handler is registered for OrderArchived.");
    assertThatThrownBy(() -> EventHandlerFixture.of(new OrderHandler(), new OrderCommands()).when(new OrderPlaced("order-1", "ada")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("An EventHandlerFixture takes no command handlers: test them with an AggregateFixture.");
  }

  /** Without Lombok, as in an application at runtime: the missing id must not become a NoClassDefFoundError. */
  @Test
  @DisplayName("Should fail like the handler when a returned command has no id")
  void commandWithoutId() {
    fixture.when(new OrderMislaid("order-1"))
        .expectFailure(AggregateIdMissingException.class,
            "The @AggregateId field orderId of io.github.alikelleci.eventify.test.EventHandlerFixtureTest$ShipParcel is null.");
  }
}
