# Domain Modeling

An aggregate is one consistency boundary. Give it a stable name and an identifier; commands and events for that aggregate carry the same id.

```java
@Value
@Builder(toBuilder = true)
@AggregateRoot("order")
public class Order {
    @AggregateId String id;
    String customer;
    String trackingNumber;
}
```

Use an immutable class. The name in `@AggregateRoot` identifies stored history, so keep it stable.

## Commands and events

Commands express intent; events express facts. Both need exactly one `@AggregateId` field. Group related types under one `@Topic`.

```java
@Topic("commands.order")
public interface OrderCommand {
    record Place(@AggregateId String id, String customer) implements OrderCommand {}
    record Ship(@AggregateId String id, String trackingNumber) implements OrderCommand {}
}

@Topic("events.order")
public interface OrderEvent {
    record Placed(@AggregateId String id, String customer) implements OrderEvent {}
    record Shipped(@AggregateId String id, String trackingNumber) implements OrderEvent {}
}
```

Use Bean Validation annotations on command fields when input validation belongs with the command.

## Rules of thumb

- Choose event names in past tense: `OrderPlaced`, not `PlaceOrder`.
- Put business time in the event payload; Eventify's timestamp is recording time.
- Never change an existing event's meaning. Add an upcaster when its shape changes.
- A removed aggregate is represented by an event whose `@ApplyEvent` method returns `null`.
