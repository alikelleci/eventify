# Advanced Features

## Snapshotting

By default, aggregate state is reconstructed by replaying its event history from the beginning. For aggregates with a long history, this can become expensive. Snapshotting improves reconstruction performance by periodically saving the current aggregate state, allowing Eventify to replay only the events that occurred after the latest snapshot.

Enable snapshotting by adding `@EnableSnapshotting` to your aggregate class:

```java
@Value
@Builder(toBuilder = true)
@AggregateRoot
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
