# Testing

`eventify-test` tests your handlers without a broker: `AggregateFixture` for aggregates, `EventHandlerFixture` for event handlers.

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-test</artifactId>
    <version>x.y.z</version>
    <scope>test</scope>
</dependency>
```

## Aggregates

Given the aggregate's past events, when a command arrives, then expect its events, its rejection or its new state.

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
| `expectState(order -> ...)` | Your assertions pass on the aggregate after the command |

Good to know:

- `given(...)` takes the events that already happened, to set up the state; leave it out for a new aggregate. `when(...)` takes the one command you test.
- Pass every handler of the aggregate, also when command and event-sourcing handlers are separate classes: `AggregateFixture.of(Order.class, new OrderCommandHandler(), new OrderEventSourcingHandler())`. Without its event-sourcing handlers, the aggregate never has a state.
- The state is `null` before the aggregate is created and after it is removed.
- Events are compared with `equals`, as they are read back from storage: use records or classes with `equals`, such as Lombok's `@Value`.
- For your own assertions: `events()` and `state()`, `cause()` with the "Exception: message" of a rejection, and `result()` with the full result.

## Event handlers

When an event arrives, then expect the commands its handlers send now or schedule.

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
| `expectCommands(...)` | Exactly these commands were sent now; `expectCommands()` for none |
| `expectScheduled(...)` | Exactly these commands were scheduled, at these times |
| `expectNoCommands()` | No commands were sent or scheduled |
| `expectFailure()` | A handler failed, and none of the commands were sent |
| `expectFailure(Exception.class)` | A handler failed by this exception, as the root cause |
| `expectFailure(Exception.class, "message")` | The same, with exactly this message |

Good to know:

- `after(duration, command)` expects the command that long after the event's timestamp, as your handler schedules it with `@Timestamp`. For a fixed time, use `Scheduled.of(command, time)`. One `expectScheduled(...)` takes all of them; times are compared to the millisecond.
- Kafka keeps the order of commands per topic, so the order is checked per topic.
- Register only event handlers: test command handlers with an `AggregateFixture`.
- For your own assertions: `commands()` and `scheduled()`, `cause()` with the "Exception: message" of a failure, and `sent()` with the commands as sent, including their metadata.

An event handler without commands, such as a projection, is tested the same way. Give it a fake to write to:

```java
InMemoryOrderOverview overview = new InMemoryOrderOverview();

EventHandlerFixture.of(new OrderOverviewProjection(overview))
    .when(new OrderPlaced("order-1", "Ada"))
    .expectNoCommands();

assertThat(overview.find("order-1").status()).isEqualTo("placed");
```

## For both fixtures

- A failed expectation throws an `AssertionError`, so it works with any test framework.
- Every `when(...)` starts from scratch, so one fixture can serve all tests of a class.
- Expectations take payloads, such as `new OrderPlaced(...)`, not a `Command` or `Event`. To check metadata, use `result()` or `sent()`.
- When your application builds Eventify with its own `objectMapper(...)`, give the fixture the same one: `AggregateFixture.of(...).withObjectMapper(objectMapper)`.
- To give the command or event in `when(...)` metadata, pass a `Command` or an `Event` instead of the payload. An event needs an aggregate type and a sequence; event handlers don't see them, so any will do:

```java
// AggregateFixture
fixture.when(Command.builder().payload(new CancelOrder("order-1"))
    .metadata(Metadata.of("user", "ada")).build());

// EventHandlerFixture
fixture.when(Event.builder().aggregateType("order").sequence(1)
    .payload(new OrderPaid("order-1")).metadata(Metadata.of("user", "ada")).build());
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
