# Domain Modeling

## Defining an Aggregate

An aggregate is a plain, immutable class annotated with `@AggregateRoot`. It represents the current state of your domain entity.

```java
@Value
@Builder(toBuilder = true)
@AggregateRoot
public class Order {
    @AggregateId
    String id;
    String customer;
    String trackingNumber;
    Instant placedAt;
}
```

- `@AggregateRoot` marks the class as an aggregate. Eventify also uses it to identify the aggregate state that can be injected into handler methods.
- `@Builder(toBuilder = true)` is recommended so that event-sourcing handlers can create updated state using `state.toBuilder()...build()`.
- The class should be immutable—use Lombok `@Value` or make all fields `final`.

## Commands and Events

Commands and events are plain, immutable value objects. The recommended pattern is to group them under a marker interface annotated with `@TopicInfo`, which declares the Kafka topic used for those messages. Every command and event class must contain exactly one `String` field annotated with `@AggregateId`. This field identifies the target aggregate instance.

### Commands

```java
@TopicInfo("commands.order")
public interface OrderCommand {

    @Value
    @Builder
    class PlaceOrder implements OrderCommand {
        @AggregateId
        String id;
        @NotBlank
        String customer;
    }

    @Value
    @Builder
    class ShipOrder implements OrderCommand {
        @AggregateId
        String id;
        @NotBlank
        String trackingNumber;
    }

    @Value
    @Builder
    class CancelOrder implements OrderCommand {
        @AggregateId
        String id;
    }
}
```

> Bean Validation annotations such as `@NotBlank` and `@Max` on command fields are enforced automatically before the handler is invoked. If validation fails, Eventify produces a command failure result without invoking the handler.

### Events

```java
@TopicInfo("events.order")
public interface OrderEvent {

    @Value
    @Builder
    class OrderPlaced implements OrderEvent {
        @AggregateId
        String id;
        String customer;
    }

    @Value
    @Builder
    class OrderShipped implements OrderEvent {
        @AggregateId
        String id;
        String trackingNumber;
    }

    @Value
    @Builder
    class OrderCancelled implements OrderEvent {
        @AggregateId
        String id;
    }
}
```
