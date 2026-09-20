# Annotation Reference

| Annotation | Where | Description |
|---|---|---|
| `@Topic("topic")` | Command / Event interface or class | Declares the Kafka topic. Inherited by all nested classes. |
| `@AggregateId` | Field | Marks the field that identifies the aggregate: a `String`, a `UUID` or a number. |
| `@AggregateRoot("order")` | Class | Marks a class as an aggregate root, under a name of your own choosing. |
| `@EnableSnapshotting` | Aggregate class | Enables periodic snapshotting. |
| `@Revision(n)` | Event payload class, aggregate class | On an event: its current schema revision, for upcasting. On an aggregate: the revision of its fields and `@ApplyEvent` methods; snapshots of another revision are not used. Defaults to `1`. |
| `@HandleCommand` | Method | Marks a command-handler method. |
| `@ApplyEvent` | Method | Marks an event-sourcing handler method. |
| `@HandleEvent` | Method | Marks an event-handler method. |
| `@Upcast(type, revision)` | Method | Marks an upcaster method for a specific event type and source revision. |
| `@Priority(n)` | `@HandleEvent` method | Controls invocation order when multiple handlers exist for the same event. Higher values run first. |
| `@Timestamp` | Method parameter | Injects the message timestamp as an `Instant`. |
| `@MessageId` | Method parameter | Injects the unique message ID as a `String`. |
| `@MetadataValue("key")` | Method parameter | Injects a specific metadata value as a `String`. |
