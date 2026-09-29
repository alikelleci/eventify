# Upgrading to 5.0

What changes for an application that runs on Eventify 4.x: in its code, in how it is deployed, and on its topics.
Eventify 5 uses the same Kafka, Jackson and Spring Boot versions as 4.0.3, so no other dependency has to move with it.

The stores of a 4.x application must be migrated once; [Migrating from 4.x](migration.md) describes how.

## Imports and annotations

Most changes are new names and packages. The prefix `io.github.alikelleci.eventify.core` is left out below.

| Eventify 4 | Eventify 5 |
|---|---|
| `common.annotations.TopicInfo` | `message.annotation.Topic` |
| `common.annotations.AggregateId` | `message.annotation.AggregateId` |
| `common.annotations.Revision` | `message.annotation.Revision` |
| `common.annotations.AggregateRoot` | `aggregate.annotation.AggregateRoot("name")`: the name is required |
| `common.annotations.EnableSnapshotting` | `aggregate.annotation.EnableSnapshotting` |
| `messaging.commandhandling.annotations.HandleCommand` | `command.annotation.CommandHandler` |
| `messaging.eventsourcing.annotations.ApplyEvent` | `aggregate.annotation.EventSourcingHandler` |
| `messaging.eventhandling.annotations.HandleEvent` | `event.annotation.EventHandler` |
| `common.annotations.Priority` | `event.annotation.Priority` |
| `messaging.upcasting.annotations.Upcast` | `upcasting.annotation.Upcaster` |
| `common.annotations.Timestamp`, `MessageId`, `MetadataValue` | `handler.annotation.Timestamp`, `MessageId`, `MetadataValue` |
| `messaging.commandhandling.Command` | `command.Command` |
| `messaging.eventhandling.Event` | `event.Event` |
| `messaging.Metadata` | `message.Metadata` |
| `messaging.commandhandling.gateway.CommandGateway` | `command.gateway.CommandGateway` |
| `messaging.commandhandling.exceptions.CommandExecutionException` | `command.exception.CommandExecutionException` |
| `common.exceptions.*` | `message.exception.*`; `TopicInfoMissingException` is now `TopicMissingException` |
| `support.serialization.json.JsonSerde`, `JsonSerializer`, `JsonDeserializer` | `serialization.*`; for events and commands prefer `event.EventSerde` and `command.CommandSerde` |
| `support.serialization.json.util.JacksonUtils.enhancedObjectMapper()` | `serialization.EventifyObjectMapper.create()` |

## The aggregate name

`@AggregateRoot` now takes a name: `@AggregateRoot("order")`. Eventify 5 stores an aggregate's events under this name,
so it must stay the same for as long as the data exists. Choose it once; renaming the class later does not move the
data, renaming the name does. The migration tool needs the same name.

## Handlers

Eventify 5 checks the handlers when they are registered, and refuses to start where Eventify 4 went on:

- **A `@CommandHandler` takes exactly one aggregate parameter**, even when it does not use the state: the aggregate
  tells Eventify which store the command belongs to. In Eventify 4 it was optional.
  ```java
  @CommandHandler
  public OrderPlaced handle(PlaceOrder command, Order state) { ... }
  ```
- **An `@EventSourcingHandler` returns exactly the type of its aggregate parameter**, not `Object` or a supertype.
- **One handler per command or event class.** Eventify 4 kept the last one it found; Eventify 5 names both and stops.
- **An `@Upcaster` takes a `JsonNode` or `ObjectNode` and returns a `JsonNode`.**

Handlers can receive the same values as before (`Metadata`, `@Timestamp`, `@MessageId`, `@MetadataValue`). Bean
validation on command payloads works as before.

## Messages

- **`Metadata` is immutable.** `Metadata.builder().put(k, v).build()` becomes `Metadata.of(k, v)`; add more with
  `.with(k, v)`.
- **A command or event gets no timestamp from the builder.** `Command.builder().timestamp(...)` is gone: the timestamp
  is when the message was made, and an event's is when it was recorded.
- **Ids are UUIDs.** An Eventify 4 id was `aggregateId@ULID`, and code could sort on it. Nothing may assume a format
  or an order now: an aggregate's events are ordered by their `sequence`.

## Command gateway

`send()` now completes with a `CommandResult.Success`, which holds the command and the events it recorded. In Eventify
4 it completed with the command payload.

```java
// Eventify 4
CompletableFuture<PlaceOrder> result = gateway.send(command);

// Eventify 5
CompletableFuture<PlaceOrder> result = gateway.send(command).thenApply(success -> command);
CompletableFuture<Void> done = gateway.send(command).thenApply(success -> null);
```

A controller that returns the future as `CompletableFuture<Object>` still compiles, but its response body changes
from the command to `{command, events}`. Check those on purpose.

Also different:

- A command whose handler records no event now succeeds at once. Eventify 4 sent no result, so the caller waited
  until its timeout (5 minutes) and got a timeout.
- A command that Kafka refuses to take fails the future at once.
- A timeout of `sendAndWait` is a `CommandTimeoutException`.

## Spring Boot

Build the `Eventify` bean from the injected builder: it has every handler bean registered already.

```java
@Bean
Eventify eventify(Eventify.EventifyBuilder builder) {
    return builder.streamsConfig(config).build();
}
```

`Eventify.builder()` gives a builder without handlers. Eventify 4 registered the handler beans on such a bean
afterwards; Eventify 5 does not, so an `Eventify` built that way handles nothing.

## Removed

| Eventify 4 | Instead |
|---|---|
| `@HandleResult`, `@HandleSuccess`, `@HandleFailure` | Read the `<topic>.results` topic with `CommandResultSerde`. |
| `EventGateway` | Events come from command handlers only. Publish other messages with a plain Kafka producer. |
| `Eventify.builder().stateListener(...)`, `.stateRestoreListener(...)` | An `EventifyPlugin`, registered with `registerPlugin`. |

Eventify 4 also stored events that other applications published on an event topic, when it had an `@ApplyEvent` for
them. Eventify 5 does not subscribe to event topics for its aggregates: an aggregate's history is what its own
commands recorded.

## Other applications on the same topics

Applications that read the event topics see a different envelope for new events. The payload and its `@class`, the
topic and the key (the aggregate id) stay the same.

| Field | Eventify 4 | Eventify 5 |
|---|---|---|
| `id` | `aggregateId@ULID` | a UUID |
| `timestamp` | the command's timestamp, from the sender's clock | when the event was recorded |
| `aggregateType`, `sequence` | not there | the aggregate name and the event's number in its aggregate |
| `metadata` | `$correlationId`, `$replyTo` | `$correlationId`, `$causationId` (the command's id) |

An application that still reads with Eventify 4 keeps working: it ignores the new fields. An application that reads
with Eventify 5 gets the events from before the upgrade without an aggregate type and with sequence 0; the events on
the topics are not migrated, only the stores are.

The `<topic>.results` topics now carry `CommandResult` JSON instead of the command with a `$result` in its metadata.
The reply to the gateway travels in a Kafka header instead of `$replyTo`.

## Deploying

Stop every instance of the Eventify 4 application before the first Eventify 5 instance starts: the two cannot run
next to each other, so a rolling update is not possible. The order is in
[Migrating from 4.x](migration.md#the-migration). Applications that only read the event topics can be upgraded
before or after it, independently.
