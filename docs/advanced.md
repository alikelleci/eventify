# Advanced Features

## Snapshotting

By default, aggregate state is reconstructed by replaying its event history from the beginning. For aggregates with a long history, this can become expensive. Snapshotting improves reconstruction performance by periodically saving the current aggregate state, allowing Eventify to replay only the events that occurred after the latest snapshot.

Enable snapshotting by adding `@EnableSnapshotting` to your aggregate class:

```java
@Value
@Builder(toBuilder = true)
@AggregateRoot
@EnableSnapshotting(threshold = 500)
public class Customer {
    // ...
}
```

| Attribute | Default | Description |
|---|---|---|
| `threshold` | `500` | A snapshot is created whenever the aggregate version reaches a multiple of this value. |
| `deleteEvents` | `false` | If `true`, events before the snapshot are deleted after the snapshot is created, reducing storage usage. |

Snapshotting is transparent to your handlers—you do not need to change any handler code.

## Event Upcasting

As your application evolves, the structure of your events may change. Upcasting lets you transparently migrate older stored event data to a newer schema without modifying the event store.

### How it works

1. Annotate your event class with `@Revision(n)` to declare its current schema version.
2. Write an upcaster method for each revision that needs to be migrated and annotate it with `@Upcast(type, revision)`.
3. When an older event is read, Eventify automatically chains the required upcasters in ascending revision order.

### Example

Suppose `CustomerCreated` started at revision 1 and is now at revision 3 after two schema changes:

```java
// Current version of the event — revision 3
@Revision(3)
@Value
@Builder
class CustomerCreated implements CustomerEvent {
    @AggregateId
    String id;
    String firstName;
    String lastName;
    String email;       // added in revision 2
    String phoneNumber; // added in revision 3
}
```

```java
public class CustomerEventUpcaster {

    // Migrates revision 1 → 2: adds a default email
    @Upcast(type = "com.example.CustomerEvent$CustomerCreated", revision = 1)
    public JsonNode upcast(ObjectNode node) {
        node.put("email", "unknown@example.com");
        return node;
    }

    // Migrates revision 2 → 3: adds a default phone number
    @Upcast(type = "com.example.CustomerEvent$CustomerCreated", revision = 2)
    public JsonNode upcast(ObjectNode node) {
        node.put("phoneNumber", "unknown");
        return node;
    }
}
```

- `type` is the fully qualified class name of the event payload. For nested classes, use `$` as the separator.
- `revision` is the **source** revision—the version stored in the event store, not the target revision.
- Events without a `@Revision` annotation default to revision `1`.
