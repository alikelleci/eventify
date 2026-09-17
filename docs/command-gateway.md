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
| `consumerConfig(Properties)` | No | Settings for the consumer that receives the results. It already takes every setting from the producer config that a consumer also has, such as `bootstrap.servers`, `security.protocol`, `sasl.*` and `ssl.*`, so this is only needed for a setting it should have differently. |
| `objectMapper(ObjectMapper)` | No | Custom Jackson `ObjectMapper`. Defaults to an enhanced mapper with common modules registered. |

The gateway holds a Kafka producer, a consumer and a thread. Close it when your application stops, for example as a Spring bean with `@Bean(destroyMethod = "close")` (Spring calls `close()` by default). Closing sends the commands still buffered and fails the futures still waiting for a result with a `CancellationException`.

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

Sending the same `Command` object again while it still waits for its result fails with an `IllegalStateException`: it would otherwise be handled twice.
