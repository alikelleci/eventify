# Advanced Features

## Snapshotting

By default, aggregate state is reconstructed by replaying its event history from the beginning. For aggregates with a long history, this can become expensive. Snapshotting improves reconstruction performance by periodically saving the current aggregate state, allowing Eventify to replay only the events that occurred after the latest snapshot.

Enable snapshotting by adding `@EnableSnapshotting` to your aggregate class:

```java
@Value
@Builder(toBuilder = true)
@AggregateRoot("order")
@EnableSnapshotting(threshold = 500)
public class Order {
    // ...
}
```

| Attribute | Default | Description |
|---|---|---|
| `threshold` | `500` | A snapshot is created whenever the aggregate version reaches or passes a multiple of this value. |
| `deleteEvents` | `false` | If `true`, events before the snapshot are deleted after the snapshot is created, reducing storage usage. |

Snapshotting is transparent to your handlers—you do not need to change any handler code.

### When the aggregate changes: `@Revision`

A snapshot holds the aggregate's state as your code computed it. When you change the aggregate's fields or its `@ApplyEvent` methods, a snapshot made before may no longer match what the current code would compute from the same events. Raise the aggregate's `@Revision` then:

```java
@AggregateRoot("order")
@Revision(2)
@EnableSnapshotting(threshold = 500)
public class Order { ... }
```

A snapshot remembers the revision it was made with. A snapshot of another revision is not used: the aggregate is rebuilt from all its events, with the current code, and snapshotted again at the next threshold. The same happens to a snapshot that can't be read at all, e.g. after moving the aggregate class to another package.

| Change | Raise `@Revision`? |
|---|---|
| An `@ApplyEvent` method computes differently | Yes |
| A field is added that the events fill | Yes |
| A field is renamed or changes type | Yes |
| The aggregate class is moved or renamed | Not needed: its snapshots can't be read and are rebuilt anyway |
| A new event with a new `@ApplyEvent` method, not touching old events | No |
| A command handler changes | No: it isn't part of the snapshot |

The number itself doesn't matter: only whether it differs from the snapshot's. Without `@Revision` an aggregate is revision 1. When in doubt, raise it: it only costs rebuilding each aggregate once, at its next command.

The revision is not the aggregate's version: the version counts the events the aggregate went through, the revision is the version of your code.

**With `deleteEvents = true`** the events before a snapshot are gone, so an outdated snapshot can't be rebuilt. Commands of such an aggregate fail with a `SnapshotOutdatedException` instead of going on with a state the current code would not compute. Think twice before combining `deleteEvents` with changes to an aggregate.

## Event Upcasting

As your application evolves, the structure of your events may change. Upcasting lets you transparently migrate older stored event data to a newer schema without modifying the event store.

### How it works

1. Annotate your event class with `@Revision(n)` to declare its current schema version.
2. Write an upcaster method for each revision that needs to be migrated and annotate it with `@Upcast(type, revision)`.
3. When an older event is read, Eventify automatically chains the required upcasters in ascending revision order.

### Example

Suppose `OrderPlaced` started at revision 1 and is now at revision 3 after two schema changes:

```java
// Current version of the event — revision 3
@Revision(3)
@Value
@Builder
class OrderPlaced implements OrderEvent {
    @AggregateId
    String id;
    String customer;
    String shippingAddress; // added in revision 2
    String couponCode;      // added in revision 3
}
```

```java
public class OrderEventUpcaster {

    // Migrates revision 1 → 2: adds a default shipping address
    @Upcast(type = "com.example.OrderEvent$OrderPlaced", revision = 1)
    public JsonNode addShippingAddress(ObjectNode payload) {
        payload.put("shippingAddress", "unknown");
        return payload;
    }

    // Migrates revision 2 → 3: adds a default coupon code
    @Upcast(type = "com.example.OrderEvent$OrderPlaced", revision = 2)
    public JsonNode addCouponCode(ObjectNode payload) {
        payload.putNull("couponCode");
        return payload;
    }
}
```

- `type` is the fully qualified class name of the event payload. For nested classes, use `$` as the separator.
- `revision` is the **source** revision—the version stored in the event store, not the target revision.
- Events without a `@Revision` annotation default to revision `1`.
- An upcaster receives the event's `payload` object only, as the previous upcaster in the chain left it.
- There can be only one upcaster per `type` and `revision`; a second one fails at startup.
- Returning `null` stops the chain: the event is read at the revision it has reached.

An upcaster may change the node it receives and return it, as above, or leave it untouched and return a new node.
Both work: every read of an event parses its own copy of the stored JSON, so changing it never affects the store or
other reads. A new node is useful when you build the new shape from scratch:

```java
// Migrates revision 1 → 2: "name" is split into "firstName" and "lastName"
@Upcast(type = "com.example.CustomerEvent$CustomerRegistered", revision = 1)
public JsonNode splitName(ObjectNode payload) {
    String[] name = payload.path("name").asText().split(" ", 2);

    ObjectNode upcasted = payload.deepCopy();
    upcasted.remove("name");
    upcasted.put("firstName", name[0]);
    upcasted.put("lastName", name.length > 1 ? name[1] : "");
    return upcasted;
}
```

A new node built without `@class` keeps the event's class.

### Renaming an event class

The stored events keep the class name they were written with. To rename or move an event class, give the new class
the next revision, and add an upcaster for the **old** class name that sets `@class` to the new one:

```java
// Revision 3 (was OrderPlaced up to revision 2)
@Revision(3)
@Value
@Builder
class OrderCreated implements OrderEvent {
    @AggregateId
    String id;
    String customer;
    String shippingAddress;
    String couponCode;
}
```

```java
// Migrates revision 2 → 3: OrderPlaced is renamed to OrderCreated
@Upcast(type = "com.example.OrderEvent$OrderPlaced", revision = 2)
public JsonNode renameToOrderCreated(ObjectNode payload) {
    payload.put("@class", "com.example.OrderEvent$OrderCreated");
    return payload;
}

// Later changes are registered for the new name
@Upcast(type = "com.example.OrderEvent$OrderCreated", revision = 3)
public JsonNode addChannel(ObjectNode payload) {
    payload.put("channel", "web");
    return payload;
}
```

- The chain continues with the upcasters of the new class name, from the revision reached. The upcasters of the old
  name for earlier revisions stay: events stored at revision 1 still need them before the rename.
- The event is read as the new class, and its `type` is the new simple name, e.g. `OrderCreated`.
- The rename and a change of fields can happen in one upcaster.
- Only reads with the upcasters registered are renamed: Eventify's event store and event handlers use the ones
  registered on Eventify. A service that reads the events topic itself, e.g. a Kafka Streams projection, registers the
  same upcasters on its serde, or still gets the old class name:

  ```java
  Serde<Event> eventSerde = new EventSerde()
      .withUpcasters(new OrderEventUpcaster());
  ```

### Where upcasters are registered

An upcaster runs only where it is registered, so every service that reads the events needs it:

- **Eventify**: register the class with `@Upcast` methods like a handler, with `registerHandler(new OrderEventUpcaster())`. With the Spring Boot starter, a bean with `@Upcast` methods is registered on the injected `Eventify.EventifyBuilder`.
- **A Kafka Streams service** that reads the events topic itself: `new EventSerde().withUpcasters(new OrderEventUpcaster())`.
- **`@KafkaListener` methods** with the Spring Boot starter use the upcasters of your `Eventify` bean, or the beans with `@Upcast` methods when there is no `Eventify` bean.

With the Spring Boot starter, an upcaster from a shared library is only picked up when it is a bean: declare it with `@Bean`, or include its package in the component scan.
