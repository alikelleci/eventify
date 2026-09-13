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
// Async — returns a CompletableFuture
CompletableFuture<PlaceOrder> future = gateway.send(
    PlaceOrder.builder().id("order-1").customer("John Doe").build()
);

// Blocking — waits up to 1 minute by default
PlaceOrder result = gateway.sendAndWait(
    PlaceOrder.builder().id("order-1").customer("John Doe").build()
);

// Blocking with a custom timeout
PlaceOrder result = gateway.sendAndWait(
    PlaceOrder.builder().id("order-1").customer("John Doe").build(),
    30, TimeUnit.SECONDS);
```

If the command fails, `sendAndWait` throws a `CommandExecutionException` containing the failure message. When using `send`, the returned future completes exceptionally with the same exception.
