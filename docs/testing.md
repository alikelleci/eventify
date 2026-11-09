# Testing

## Aggregates

`eventify-test` tests your handlers without a broker. `AggregateFixture` tests the handlers of one aggregate: given its past events, when a command arrives, then expect its events, its rejection or its new state.

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-test</artifactId>
    <version>x.y.z</version>
    <scope>test</scope>
</dependency>
```

```java
AggregateFixture<Order> fixture = AggregateFixture.of(Order.class, new OrderHandler());

@Test
void placesAnOrder() {
    fixture.when(new PlaceOrder("order-1", "Ada"))
        .expectEvents(new OrderPlaced("order-1", "Ada"))
        .expectState(order -> assertThat(order.customer()).isEqualTo("Ada"));
}

@Test
void cancelsAPlacedOrder() {
    fixture.given(new OrderPlaced("order-1", "Ada"))
        .when(new CancelOrder("order-1"))
        .expectEvents(new OrderCancelled("order-1"));
}

@Test
void rejectsAnUnknownOrder() {
    fixture.when(new CancelOrder("order-1"))
        .expectRejection(OrderNotFoundException.class);
}
```

| Expectation | Passes when |
|---|---|
| `expectSuccess()` | The command was accepted |
| `expectEvents(...)` | The command was accepted with exactly these events, in this order |
| `expectNoEvents()` | The command was accepted without events |
| `expectRejection()` | The command was rejected |
| `expectRejection(Exception.class)` | The command was rejected by this exception, as the root cause |
| `expectRejection(Exception.class, "message")` | The same, with exactly this message |
| `expectState(order -> ...)` | Your assertions pass on the aggregate after the command; it is `null` before the aggregate is created or after it is removed |

A failed expectation throws an `AssertionError`, so it works with any test framework. For your own assertions, use `result()`, `events()`, `state()` and `cause()`, the "Exception: message" of a rejection.

`expectEvents(...)` compares with `equals`, on events read back as they are stored. Use records or classes with `equals`, such as Lombok's `@Value`.

Pass a `Command` to `when(...)` to give it metadata. Every scenario starts without the events of an earlier one, so one fixture can serve all tests of a class.

Pass every handler of the aggregate, also when your command and event-sourcing handlers are in separate classes. Without its event-sourcing handlers, the aggregate never has a state.

```java
AggregateFixture<Order> fixture = AggregateFixture.of(Order.class,
    new OrderCommandHandler(), new OrderEventSourcingHandler());
```

When your application builds Eventify with its own `objectMapper(...)`, give the fixture the same one, so events are stored as in your application:

```java
AggregateFixture<Order> fixture = AggregateFixture.of(Order.class, new OrderHandler())
    .withObjectMapper(objectMapper);
```

## Event handlers

`EventHandlerFixture` tests event handlers that send commands: when an event arrives, then expect the commands they send now or schedule.

```java
import static io.github.alikelleci.eventify.test.EventHandlerFixture.after;

EventHandlerFixture fixture = EventHandlerFixture.of(new PaymentHandler());

@Test
void requestsPaymentAndCancelsWhenUnpaid() {
    fixture.when(new OrderPlaced("order-1", "Ada"))
        .expectCommands(new RequestPayment("order-1"))
        .expectScheduled(after(Duration.ofMinutes(30), new CancelUnpaidOrder("order-1")));
}

@Test
void failsForALostOrder() {
    fixture.when(new OrderLost("order-1"))
        .expectFailure(IllegalStateException.class);
}
```

| Expectation | Passes when |
|---|---|
| `expectSuccess()` | The event was handled |
| `expectCommands(...)` | Exactly these commands were sent now; `expectCommands()` for none, whatever was scheduled |
| `expectScheduled(...)` | Exactly these commands were scheduled, at these times to the millisecond |
| `expectNoCommands()` | The event was handled without sending or scheduling commands |
| `expectFailure()` | A handler failed, and none of the commands were sent |
| `expectFailure(Exception.class)` | A handler failed by this exception, as the root cause |
| `expectFailure(Exception.class, "message")` | The same, with exactly this message |

Give `expectScheduled(...)` each command with its time: `after(Duration.ofMinutes(30), command)` for 30 minutes after the event's timestamp, as your handler does with `@Timestamp`, or `Scheduled.of(command, time)` for a fixed time. One call takes all of them.

Kafka keeps the order of commands per topic, so the expectations check the order per topic; commands for different topics can be listed in any order.

For your own assertions, use `commands()`, `scheduled()`, `cause()` and `sent()`, the commands as sent with their metadata. Pass an `Event` to `when(...)` to give the event metadata. Register only event handlers: test command handlers with an `AggregateFixture`.

An event handler that sends no commands, such as a projection, is tested the same way. Give it a fake of what it writes to, and check that afterwards:

```java
InMemoryOrderOverview overview = new InMemoryOrderOverview();

EventHandlerFixture.of(new OrderOverviewProjection(overview))
    .when(new OrderPlaced("order-1", "Ada"))
    .expectNoCommands();

assertThat(overview.find("order-1").status()).isEqualTo("placed");
```

## The complete flow

To test how aggregates and event handlers work together, use Kafka Streams' `TopologyTestDriver` yourself. It's in `eventify-test` too, or add `org.apache.kafka:kafka-streams-test-utils`.

```java
Properties testConfig = new Properties();
testConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-test");
testConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

Eventify eventify = Eventify.builder()
    .streamsConfig(testConfig)
    .registerHandler(new OrderHandler())
    .build();

try (TopologyTestDriver driver = new TopologyTestDriver(eventify.topology())) {
    var commands = driver.createInputTopic(
        "commands.order", new StringSerializer(), new CommandSerde().serializer());
    var results = driver.createOutputTopic(
        "commands.order.results", new StringDeserializer(), new CommandResultSerde().deserializer());
    var events = driver.createOutputTopic(
        "events.order", new StringDeserializer(), new EventSerde().deserializer());

    Command command = Command.builder().payload(new PlaceOrder("order-1", "Ada")).build();
    commands.pipeInput(command.getAggregateId(), command);

    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
    assertThat(events.readValue().getPayload()).isEqualTo(new OrderPlaced("order-1", "Ada"));
}
```

Test both successful decisions and rejected business rules. Assert the command result and the events your application publishes.
