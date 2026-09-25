# Advanced Features

## Snapshots

Eventify rebuilds an aggregate from its events. Add a snapshot when a long history makes that too expensive.

```java
@AggregateRoot("order")
@EnableSnapshotting(threshold = 500)
public class Order { ... }
```

Eventify saves a snapshot after a successful command when the threshold is crossed. Handlers need no special code.

| Setting | Meaning |
|---|---|
| `threshold` | Create a snapshot every N events. |
| `deleteEvents` | Remove events before the snapshot after it is safely stored. Defaults to `false`. |

Keep `deleteEvents` disabled when you need full audit history or time travel before the snapshot. When it is enabled, Eventify fails safely if an old snapshot cannot be used.

### Aggregate changes

Raise the aggregate's `@Revision` when a changed field or `@EventSourcingHandler` method would produce a different state from the same events.

```java
@AggregateRoot("order")
@Revision(2)
@EnableSnapshotting(threshold = 500)
public class Order { ... }
```

Old snapshots are then ignored and rebuilt from events when possible.

## Event upcasting

Upcasters transform old event JSON into the current event shape while reading history. Increase an event's `@Revision` and add one `@Upcaster` method per step.

```java
@Revision(2)
public record OrderPlaced(@AggregateId String id, String customer, String channel) {}

public class OrderUpcaster {
    @Upcaster(type = "com.example.OrderPlaced", revision = 1)
    public JsonNode addChannel(ObjectNode json) {
        json.put("channel", "web");
        return json;
    }
}
```

Register the upcaster like any other handler. Keep old upcasters: older events may still need every step in the chain.
