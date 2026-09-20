# Command Gateway

The `CommandGateway` is the client-side component used to send commands and receive their results. It is typically used in your API layer, such as a REST controller, to dispatch commands to Eventify and await their outcome.

## Configuration

```java
Properties producerConfig = new Properties();
producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

CommandGateway gateway = CommandGateway.builder()
    .producerConfig(producerConfig)
    .replyTopic("my-app.replies")
    .build();
```

### Builder options

| Method | Required | Description |
|---|---|---|
| `producerConfig(Properties)` | Yes | Kafka producer configuration. |
| `replyTopic(String)` | Yes | Topic on which command results are received. |
| `objectMapper(ObjectMapper)` | No | Custom Jackson `ObjectMapper`. Defaults to an enhanced mapper with common modules registered. |

## Sending Commands

```java
// Async — returns a CompletableFuture with the result
CompletableFuture<CommandResult.Success> future = gateway.send(
    PlaceOrder.builder().id("order-1").customer("John Doe").build()
);

// Blocking — waits up to 1 minute by default
CommandResult.Success result = gateway.sendAndWait(
    PlaceOrder.builder().id("order-1").customer("John Doe").build()
);

// Blocking with a custom timeout
CommandResult.Success result = gateway.sendAndWait(
    PlaceOrder.builder().id("order-1").customer("John Doe").build(),
    30, TimeUnit.SECONDS);
```

### Metadata

To send something along with a command, such as the tenant or the user it is for, build the command yourself:

```java
gateway.send(Command.builder()
    .payload(PlaceOrder.builder().id("order-1").customer("John Doe").build())
    .metadata(Metadata.of("tenant", "acme").with("user", "ada"))
    .build());
```

The events the command produces carry this metadata too. Eventify adds `$correlationId` and `$causationId` itself; pass a `$correlationId` of your own to make this command part of a flow you already started.

A `Metadata` never changes: `with(...)` gives you a new one, so the same metadata can be used for more than one command.

A result holds the command and the events it produced, as they were stored and sent: `result.command()` and `result.events()`. A command accepted without events has an empty list.

The result is sent as one Kafka message, with all its events. A command that produces very many events can make it larger than Kafka's maximum message size (`max.request.size`, 1 MB by default): keep commands to one decision each, or raise the limit.

In a Spring controller, return only what the caller needs, e.g. the ids of the events:

```java
@PostMapping("/orders")
public CompletableFuture<List<String>> placeOrder(@RequestBody PlaceOrder placeOrder) {
  return gateway.send(placeOrder)
      .thenApply(result -> result.events().stream().map(Event::getId).toList());
}
```

If the command fails, `sendAndWait` throws a `CommandExecutionException` containing the failure message. When using `send`, the returned future completes exceptionally with the same exception.

When no result arrives in time, `sendAndWait` throws a `CommandTimeoutException`. The command may still be handled: only the wait for its result ended. When using `send`, the future completes exceptionally with a `TimeoutException` after five minutes without a result.

Sending the same `Command` object again while it still waits for its result fails with an `IllegalStateException`: it would otherwise be handled twice.
