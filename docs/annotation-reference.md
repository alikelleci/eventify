# Annotation Reference

| Annotation | Use |
|---|---|
| `@Topic("...")` | Topic for a command or event type. |
| `@AggregateId` | The aggregate identifier field on a command, event or aggregate. |
| `@AggregateRoot("...")` | Stable name of an aggregate type. |
| `@HandleCommand` | Decides which events a command produces. |
| `@ApplyEvent` | Produces the next aggregate state from an event. |
| `@HandleEvent` | Reacts to a published event outside the aggregate. |
| `@EnableSnapshotting` | Enables periodic snapshots. |
| `@Revision(n)` | Version of an event schema or aggregate snapshot shape. |
| `@Upcast(type, revision)` | Migrates an older event shape while reading it. |
| `@Priority(n)` | Order for handlers of the same event; high runs first. |
| `@Timestamp` | Injects a message timestamp. |
| `@MessageId` | Injects a message id. |
| `@MetadataValue("key")` | Injects one metadata value. |
