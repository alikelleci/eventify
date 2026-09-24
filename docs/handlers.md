# Handlers

Handlers are plain Java methods. Register the object that contains them with `Eventify`.

## Handle a command

A command handler receives a command and the current aggregate state. Return one event, a list of events, or no event.

```java
@HandleCommand
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
@ApplyEvent
public Order apply(OrderShipped event, Order state) {
    return state.toBuilder().trackingNumber(event.trackingNumber()).build();
}
```

Keep `@ApplyEvent` methods deterministic and side-effect free. They run whenever Eventify rebuilds state. Return `null` to remove an aggregate.

An event without an `@ApplyEvent` method still becomes part of the history and advances the aggregate version; it simply leaves state unchanged.

## React to an event

Use `@HandleEvent` for work outside the aggregate, such as projections or notifications.

```java
@HandleEvent
public void on(OrderPlaced event) {
    ordersView.insert(event.id(), event.customer());
}
```

Make side effects idempotent. A handler may run again after recovery.

## Available message values

Besides the payload and aggregate state, handlers can receive:

| Parameter | Value |
|---|---|
| `@AggregateRoot` type | Current state; required for `@HandleCommand` |
| `Metadata` | All message metadata |
| `@Timestamp Instant` | Message timestamp |
| `@MessageId String` | Message id |
| `@MetadataValue("key") String` | One metadata value |

## Ordering

Use `@Priority` only when multiple `@HandleEvent` methods handle the same event. Higher values run first.

## Spring listeners

The Spring Boot starter can also deserialize Eventify events for `@KafkaListener` methods. Use that when a projection should have its own lifecycle and error handling; command handling always uses `@HandleCommand`.
