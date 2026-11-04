# Handlers

Handlers are plain Java methods. Register the object that contains them with `Eventify`.

## Handle a command

A command handler receives a command and the current aggregate state. Return one event, a list of events, or no event.

```java
@CommandHandler
public OrderEvent handle(ShipOrder command, Order state) {
    if (state == null) throw new ValidationException("Order does not exist");
    if (state.trackingNumber() != null) return null;
    return new OrderShipped(command.id(), command.trackingNumber());
}
```

Throw an exception to reject the command. A successful command with no events is allowed.

## Apply an event

An event-sourcing handler returns the next aggregate state.

```java
@EventSourcingHandler
public Order handle(OrderShipped event, Order state) {
    return new Order(state.id(), state.customer(), event.trackingNumber());
}
```

Keep `@EventSourcingHandler` methods deterministic and side-effect free. They run whenever Eventify rebuilds state. Return `null` to remove an aggregate.

Events without a matching method are recorded but do not change aggregate state.

## React to an event

Use `@EventHandler` for work outside the aggregate, such as projections or notifications.

```java
@EventHandler
public void handle(OrderPlaced event) {
    ordersView.insert(event.id(), event.customer());
}
```

Make side effects idempotent. A handler may run again after recovery.

## Send a command from an event

An event handler can return one command, a list of commands, or `null`. Eventify sends them to their `@Topic` in the same transaction as reading the event, with the event's correlation id and the event as cause.

```java
@EventHandler
public RequestPayment handle(OrderPlaced event) {
    return new RequestPayment(event.paymentId(), event.id(), event.amount());
}
```

Use this to let one aggregate's event lead to a command for another. The aggregate keeps the state; the handler only decides which command follows. A handler that returns a class without `@Topic` is refused at startup.

## Available message values

Besides the payload and aggregate state, handlers can receive:

| Parameter | Value |
|---|---|
| `@AggregateRoot` type | Current state; allowed in command and event-sourcing handlers, required in command handlers |
| `Metadata` | All message metadata |
| `@Timestamp Instant` | Message timestamp |
| `@MessageId String` | Message id |
| `@MetadataValue("key") String` | One metadata value |

## Ordering

Use `@Priority` only when multiple `@EventHandler` methods handle the same event. Higher values run first.

## Spring listeners

With Spring Kafka on the classpath, the Spring Boot starter can also deserialize Eventify events for `@KafkaListener` methods. Select its container factory:

```java
@KafkaListener(topics = "events.order", groupId = "orders-view",
               containerFactory = "eventifyListenerContainerFactory")
public void handle(OrderPlaced event) {
    ordersView.insert(event.id(), event.customer());
}
```

Use this when a projection should have its own lifecycle and error handling; command handling always uses `@CommandHandler`.

To read events in a projection of your own, e.g. a Kafka Streams application, inject the starter's `EventSerde` bean. It reads events with the same upcasters as Eventify.

```java
builder.stream("events.order", Consumed.with(Serdes.String(), eventSerde));
```
