package io.github.alikelleci.eventify.test;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Aggregate fixture")
class AggregateFixtureTest {

  @AggregateRoot("order")
  public record Order(@AggregateId String id, String customer, boolean cancelled) {
  }

  @Topic("commands.order")
  public interface OrderCommand {
  }

  @Topic("events.order")
  public interface OrderEvent {
  }

  public record PlaceOrder(@AggregateId String id, String customer) implements OrderCommand {
  }

  public record CancelOrder(@AggregateId String id) implements OrderCommand {
  }

  /** Changes nothing. */
  public record ViewOrder(@AggregateId String id) implements OrderCommand {
  }

  /** Has no handler. */
  public record ShipOrder(@AggregateId String id) implements OrderCommand {
  }

  public record OrderPlaced(@AggregateId String id, String customer) implements OrderEvent {
  }

  public record OrderCancelled(@AggregateId String id, String reason) implements OrderEvent {
  }

  public record PriceOrder(@AggregateId String id, long cents) implements OrderCommand {
  }

  /** Has a price the default ObjectMapper can't read back. */
  public record OrderPriced(@AggregateId String id, Money price) implements OrderEvent {
  }

  /** Without a module, Jackson writes it as {} and can't create it again. */
  public static final class Money {
    private final long cents;

    Money(long cents) {
      this.cents = cents;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Money money && money.cents == cents;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(cents);
    }

    @Override
    public String toString() {
      return cents + " cents";
    }
  }

  @AggregateRoot("customer")
  public record Customer(@AggregateId String id) {
  }

  @Topic("commands.customer")
  public record RegisterCustomer(@AggregateId String id) {
  }

  @Topic("events.customer")
  public record CustomerRegistered(@AggregateId String id) {
  }

  public static class CustomerHandler {
    @CommandHandler
    public CustomerRegistered handle(RegisterCustomer command, Customer state) {
      return new CustomerRegistered(command.id());
    }

    @EventSourcingHandler
    public Customer handle(CustomerRegistered event) {
      return new Customer(event.id());
    }
  }

  /** Command handlers only, as an application may split them from its event-sourcing handlers. */
  public static class OrderCommands {
    private final OrderHandler handler = new OrderHandler();

    @CommandHandler
    public OrderPlaced handle(PlaceOrder command, Order state) {
      return handler.handle(command, state);
    }

    @CommandHandler
    public OrderCancelled handle(CancelOrder command, Order state, Metadata metadata) {
      return handler.handle(command, state, metadata);
    }
  }

  public static class OrderEventSourcing {
    @EventSourcingHandler
    public Order handle(OrderPlaced event) {
      return new Order(event.id(), event.customer(), false);
    }

    @EventSourcingHandler
    public Order handle(OrderCancelled event, Order state) {
      return new Order(state.id(), state.customer(), true);
    }
  }

  public static class OrderNotFound extends RuntimeException {
    OrderNotFound(String id) {
      super("Order " + id + " does not exist.");
    }
  }

  public static class OrderHandler {
    @CommandHandler
    public OrderPlaced handle(PlaceOrder command, Order state) {
      if (state != null) {
        throw new IllegalStateException("Order " + command.id() + " is already placed.");
      }
      return new OrderPlaced(command.id(), command.customer());
    }

    @CommandHandler
    public OrderCancelled handle(CancelOrder command, Order state, Metadata metadata) {
      if (state == null) {
        throw new OrderNotFound(command.id());
      }
      if (state.cancelled()) {
        throw new IllegalStateException("Order " + command.id() + " is already cancelled.");
      }
      return new OrderCancelled(command.id(), metadata.get("reason"));
    }

    @CommandHandler
    public Object handle(ViewOrder command, Order state) {
      return null;
    }

    @CommandHandler
    public OrderPriced handle(PriceOrder command, Order state) {
      return new OrderPriced(command.id(), new Money(command.cents()));
    }

    @EventSourcingHandler
    public Order handle(OrderPlaced event) {
      return new Order(event.id(), event.customer(), false);
    }

    @EventSourcingHandler
    public Order handle(OrderCancelled event, Order state) {
      return new Order(state.id(), state.customer(), true);
    }
  }

  private final AggregateFixture<Order> fixture = AggregateFixture.of(Order.class, new OrderHandler());

  @Test
  @DisplayName("Should give the events of a command for a new aggregate")
  void eventsOfANewAggregate() {
    fixture.when(new PlaceOrder("order-1", "Ada"))
        .expectEvents(new OrderPlaced("order-1", "Ada"))
        .expectState(order -> assertThat(order).isEqualTo(new Order("order-1", "Ada", false)));
  }

  @Test
  @DisplayName("Should handle the command on the state of the past events")
  void stateOfThePastEvents() {
    fixture.given(new OrderPlaced("order-1", "Ada"))
        .when(new CancelOrder("order-1"))
        .expectEvents(new OrderCancelled("order-1", null))
        .expectState(order -> assertThat(order.cancelled()).isTrue());
  }

  @Test
  @DisplayName("Should give each aggregate in the past events its own history")
  void pastEventsOfSeveralAggregates() {
    fixture.given(new OrderPlaced("order-1", "Ada"), new OrderPlaced("order-2", "Bob"), new OrderCancelled("order-1", null))
        .when(new CancelOrder("order-2"))
        .expectEvents(new OrderCancelled("order-2", null))
        .expectState(order -> assertThat(order).isEqualTo(new Order("order-2", "Bob", true)));

    fixture.given(new OrderPlaced("order-1", "Ada"), new OrderPlaced("order-2", "Bob"), new OrderCancelled("order-1", null))
        .when(new CancelOrder("order-1"))
        .expectRejection(IllegalStateException.class);
  }

  @Test
  @DisplayName("Should hand the command's metadata to the handler")
  void commandWithMetadata() {
    Command command = Command.builder().payload(new CancelOrder("order-1")).metadata(Metadata.of("reason", "too late")).build();

    fixture.given(new OrderPlaced("order-1", "Ada"))
        .when(command)
        .expectEvents(new OrderCancelled("order-1", "too late"));
  }

  @Test
  @DisplayName("Should give a rejection and leave the state as it was")
  void rejection() {
    fixture.given(new OrderPlaced("order-1", "Ada"), new OrderCancelled("order-1", null))
        .when(new CancelOrder("order-1"))
        .expectRejection()
        .expectRejection(IllegalStateException.class)
        .expectState(order -> assertThat(order).isEqualTo(new Order("order-1", "Ada", true)));
  }

  @Test
  @DisplayName("Should match a rejection by a nested exception class, with no state before the aggregate is created")
  void rejectionByANestedException() {
    Then<Order> then = fixture.when(new CancelOrder("order-1"))
        .expectRejection(OrderNotFound.class);

    assertThat(then.state()).isNull();
    assertThat(then.events()).isEmpty();
    assertThat(then.result()).isInstanceOf(CommandResult.Failure.class);
  }

  @Test
  @DisplayName("Should accept a command without events")
  void noEvents() {
    fixture.given(new OrderPlaced("order-1", "Ada"))
        .when(new ViewOrder("order-1"))
        .expectSuccess()
        .expectNoEvents();
  }

  @Test
  @DisplayName("Should fail when the events differ")
  void otherEvents() {
    Then<Order> then = fixture.when(new PlaceOrder("order-1", "Ada"));

    assertThatThrownBy(() -> then.expectEvents(new OrderPlaced("order-1", "Bob")))
        .isInstanceOf(AssertionError.class)
        .hasMessage("Expected PlaceOrder to give events [OrderPlaced[id=order-1, customer=Bob]],"
            + " but it gave [OrderPlaced[id=order-1, customer=Ada]].");
    assertThatThrownBy(then::expectNoEvents).isInstanceOf(AssertionError.class);
    assertThatThrownBy(then::expectRejection)
        .isInstanceOf(AssertionError.class)
        .hasMessage("Expected PlaceOrder to be rejected, but it succeeded with events [OrderPlaced[id=order-1, customer=Ada]].");
  }

  @Test
  @DisplayName("Should fail when a rejected command is expected to succeed, or rejected by another exception")
  void unexpectedRejection() {
    Then<Order> then = fixture.given(new OrderPlaced("order-1", "Ada")).when(new PlaceOrder("order-1", "Ada"));

    assertThatThrownBy(then::expectSuccess)
        .isInstanceOf(AssertionError.class)
        .hasMessage("Expected PlaceOrder to succeed, but it was rejected: IllegalStateException: Order order-1 is already placed.");
    assertThatThrownBy(() -> then.expectEvents(new OrderPlaced("order-1", "Ada"))).isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> then.expectRejection(OrderNotFound.class))
        .isInstanceOf(AssertionError.class)
        .hasMessage("Expected PlaceOrder to be rejected with AggregateFixtureTest.OrderNotFound,"
            + " but it was rejected with: IllegalStateException: Order order-1 is already placed.");
    assertThatThrownBy(() -> then.expectRejection(IllegalArgumentException.class)).isInstanceOf(AssertionError.class);
  }

  @Test
  @DisplayName("Should refuse a command without a handler")
  void commandWithoutAHandler() {
    assertThatThrownBy(() -> fixture.when(new ShipOrder("order-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No command handler is registered for ShipOrder.");
  }

  @Test
  @DisplayName("Should refuse a class that is no aggregate, or an aggregate without handlers")
  void noAggregate() {
    assertThatThrownBy(() -> AggregateFixture.of(String.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("java.lang.String is no aggregate: annotate it with @AggregateRoot.");
    assertThatThrownBy(() -> AggregateFixture.of(Order.class).when(new PlaceOrder("order-1", "Ada")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No handler is registered for aggregate Order.");
  }

  @Test
  @DisplayName("Should start every scenario without the events of an earlier one")
  void scenariosAreIndependent() {
    fixture.when(new PlaceOrder("order-1", "Ada")).expectSuccess();
    fixture.when(new PlaceOrder("order-1", "Ada")).expectSuccess();
    assertThat(fixture.when(new PlaceOrder("order-2", "Bob")).events()).isEqualTo(List.of(new OrderPlaced("order-2", "Bob")));
  }

  @Test
  @DisplayName("Should take command and event-sourcing handlers from separate classes")
  void separateHandlerClasses() {
    AggregateFixture<Order> separate = AggregateFixture.of(Order.class, new OrderCommands(), new OrderEventSourcing());

    separate.given(new OrderPlaced("order-1", "Ada"))
        .when(new CancelOrder("order-1"))
        .expectEvents(new OrderCancelled("order-1", null))
        .expectState(order -> assertThat(order).isEqualTo(new Order("order-1", "Ada", true)));
  }

  @Test
  @DisplayName("Should refuse a command of another aggregate")
  void commandOfAnotherAggregate() {
    AggregateFixture<Order> both = AggregateFixture.of(Order.class, new OrderHandler(), new CustomerHandler());

    assertThatThrownBy(() -> both.when(new RegisterCustomer("ada")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("RegisterCustomer is no command of aggregate Order.");
    both.when(new PlaceOrder("order-1", "Ada")).expectEvents(new OrderPlaced("order-1", "Ada"));
  }

  @Test
  @DisplayName("Should write events with the application's ObjectMapper")
  void applicationObjectMapper() {
    SimpleModule money = new SimpleModule()
        .addSerializer(Money.class, new JsonSerializer<>() {
          @Override
          public void serialize(Money value, JsonGenerator generator, SerializerProvider provider) throws IOException {
            generator.writeNumber(value.cents);
          }
        })
        .addDeserializer(Money.class, new JsonDeserializer<>() {
          @Override
          public Money deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            return new Money(parser.getLongValue());
          }
        });
    ObjectMapper objectMapper = EventifyObjectMapper.create().registerModule(money);

    fixture.withObjectMapper(objectMapper)
        .given(new OrderPlaced("order-1", "Ada"))
        .when(new PriceOrder("order-1", 1250))
        .expectEvents(new OrderPriced("order-1", new Money(1250)));
    fixture.given(new OrderPlaced("order-1", "Ada"))
        .when(new PriceOrder("order-1", 1250))
        .expectRejection();
  }

  @Test
  @DisplayName("Should give the cause of a rejection, and none of an accepted command")
  void cause() {
    assertThat(fixture.when(new CancelOrder("order-1")).cause())
        .isEqualTo("AggregateFixtureTest.OrderNotFound: Order order-1 does not exist.");
    assertThat(fixture.when(new PlaceOrder("order-1", "Ada")).cause()).isNull();
  }

  @Test
  @DisplayName("Should match a rejection by its exception and message")
  void rejectionWithMessage() {
    Then<Order> then = fixture.when(new CancelOrder("order-1"))
        .expectRejection(OrderNotFound.class, "Order order-1 does not exist.");

    assertThatThrownBy(() -> then.expectRejection(OrderNotFound.class, "Order order-2 does not exist."))
        .isInstanceOf(AssertionError.class)
        .hasMessage("Expected CancelOrder to be rejected with \"AggregateFixtureTest.OrderNotFound: Order order-2 does not exist.\","
            + " but it was rejected with: AggregateFixtureTest.OrderNotFound: Order order-1 does not exist.");
    assertThatThrownBy(() -> then.expectRejection(IllegalStateException.class, "Order order-1 does not exist."))
        .isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> fixture.when(new PlaceOrder("order-1", "Ada")).expectRejection(OrderNotFound.class, "Order order-1 does not exist."))
        .isInstanceOf(AssertionError.class)
        .hasMessageStartingWith("Expected PlaceOrder to be rejected, but it succeeded");
  }
}
