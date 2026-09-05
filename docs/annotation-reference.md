# Annotation Reference

| Annotation | Where | Description |
|---|---|---|
| `@TopicInfo("topic")` | Command / Event interface or class | Declares the Kafka topic. Inherited by all nested classes. |
| `@AggregateId` | Field | Marks the `String` field that identifies the aggregate. |
| `@AggregateRoot` | Class | Marks a class as an aggregate root. |
| `@EnableSnapshotting` | Aggregate class | Enables periodic snapshotting. |
| `@Revision(n)` | Event payload class | Declares the current schema revision. Defaults to `1`. |
| `@HandleCommand` | Method | Marks a command-handler method. |
| `@ApplyEvent` | Method | Marks an event-sourcing handler method. |
| `@HandleEvent` | Method | Marks an event-handler method. |
| `@Upcast(type, revision)` | Method | Marks an upcaster method for a specific event type and source revision. |
| `@Priority(n)` | `@HandleEvent` method | Controls invocation order when multiple handlers exist for the same event. Higher values run first. |
| `@Timestamp` | Method parameter | Injects the message timestamp as an `Instant`. |
| `@MessageId` | Method parameter | Injects the unique message ID as a `String`. |
| `@MetadataValue("key")` | Method parameter | Injects a specific metadata value as a `String`. |
