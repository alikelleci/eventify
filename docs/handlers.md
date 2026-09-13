# Handlers

## Command Handlers

Create a plain class and annotate its command-handling methods with `@HandleCommand`. The first parameter is always the command payload. Eventify automatically injects the remaining parameters.

```java
public class OrderCommandHandler {

    @HandleCommand
    public OrderEvent handle(PlaceOrder command, Order state) {
        if (state != null) {
            throw new ValidationException("Order already exists.");
        }
        return OrderPlaced.builder()
            .id(command.getId())
            .customer(command.getCustomer())
            .build();
    }

    @HandleCommand
    public OrderEvent handle(ShipOrder command, Order state) {
        if (state == null) {
            throw new ValidationException("Order does not exist.");
        }
        return OrderShipped.builder()
            .id(command.getId())
            .trackingNumber(command.getTrackingNumber())
            .build();
    }

    @HandleCommand
    public OrderEvent handle(CancelOrder command, Order state) {
        if (state == null) {
            throw new ValidationException("Order does not exist.");
        }
        return OrderCancelled.builder()
            .id(command.getId())
            .build();
    }
}
```

### Return values

| Return type | Behavior |
|---|---|
| A single event payload | One event is recorded and published. |
| A `List` of event payloads | Multiple events are recorded and published. |
| `null` | No events are produced, and no result is forwarded. |

### Throwing exceptions

Throw any exception to signal a business-rule failure. Eventify catches the exception and produces a failure result containing its message. Your handler does not need to create failure responses manually.

### Injectable parameters

In addition to the command payload and aggregate state, you can declare the following injectable parameters in any order:

```java
@HandleCommand
public OrderEvent handle(PlaceOrder command,
                          Order state,
                          Metadata metadata,
                          @Timestamp Instant timestamp,
                          @MessageId String messageId,
                          @MetadataValue("$correlationId") String correlationId) {
    // ...
}
```

| Parameter | What is injected |
|---|---|
| Type annotated with `@AggregateRoot` | The current aggregate state, or `null` if the aggregate does not yet exist. |
| `Metadata` | The complete metadata map for the command. |
| `@Timestamp Instant` | The command timestamp. |
| `@MessageId String` | The unique ID of the command message. |
| `@MetadataValue("key") String` | A specific value from the metadata map. |

## Event Sourcing Handlers

Create a plain class and annotate its event-sourcing methods with `@ApplyEvent`. These methods define how each event is applied to produce the next aggregate state. The first parameter is the event payload; all remaining parameters are resolved by type and can appear in any order.

```java
public class OrderEventSourcingHandler {

    @ApplyEvent
    public Order apply(OrderPlaced event, Order state) {
        return Order.builder()
            .id(event.getId())
            .customer(event.getCustomer())
            .placedAt(Instant.now())
            .build();
    }

    @ApplyEvent
    public Order apply(OrderShipped event, Order state) {
        return state.toBuilder()
            .trackingNumber(event.getTrackingNumber())
            .build();
    }

    @ApplyEvent
    public Order apply(OrderCancelled event, Order state) {
        return null; // returning null signals the aggregate no longer exists
    }
}
```

- Always return a **new** state object—never mutate the existing one.
- Return `null` to indicate that the aggregate has been deleted. Subsequent commands will receive `null` as the aggregate state.

### Injectable parameters

| Parameter | What is injected |
|---|---|
| Type annotated with `@AggregateRoot` | The current aggregate state, or `null` if the aggregate does not yet exist. |
| `Metadata` | The complete metadata map for the event. |
| `@Timestamp Instant` | The event timestamp. |
| `@MessageId String` | The unique ID of the event message. |
| `@MetadataValue("key") String` | A specific value from the metadata map. |

## Event Handlers

Create a plain class and annotate methods with `@HandleEvent` to react to published events. Event handlers are typically used for side effects such as updating a read model, sending a notification, or triggering a downstream process.

```java
public class OrderEventHandler {

    @HandleEvent
    public void on(OrderPlaced event) {
        // e.g. insert into a read model database
    }

    @HandleEvent
    public void on(OrderShipped event) {
        // e.g. update the read model
    }

    @HandleEvent
    public void on(OrderCancelled event) {
        // e.g. remove from the read model
    }
}
```

### Handler priority

If multiple handlers process the same event type and you need to control their execution order, use `@Priority`. Handlers with a higher priority value are invoked first.

```java
@HandleEvent
@Priority(10)
public void on(OrderPlaced event) {
    // invoked before handlers with lower priority
}
```

### Injectable parameters

| Parameter | What is injected |
|---|---|
| `Metadata` | The complete metadata map for the event. |
| `@Timestamp Instant` | The event timestamp. |
| `@MessageId String` | The unique ID of the event message. |
| `@MetadataValue("key") String` | A specific value from the metadata map. |
