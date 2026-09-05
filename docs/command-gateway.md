# Command Gateway

The `CommandGateway` is the client-side component used to send commands and receive their results. It is typically used in your API layer, such as a REST controller, to dispatch commands to Eventify and await their outcome.

Internally, the gateway produces commands to Kafka and listens for replies on a dedicated reply topic (partition 0). Each command is correlated to its reply by message ID using an in-memory cache that expires after 5 minutes.

## Configuration

Both `producerConfig` and `replyTopic` are required. The `replyTopic` must match the topic configured in the Eventify application that processes the commands, and it must exist in Kafka before the gateway starts.

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
| `producerConfig(Properties)` | Yes | Kafka producer configuration. Bootstrap servers are also used for the internal reply consumer. |
| `replyTopic(String)` | Yes | Topic on which command results are received. Must exist in Kafka. Always consumed from partition 0. |
| `objectMapper(ObjectMapper)` | No | Custom Jackson `ObjectMapper`. Defaults to an enhanced mapper with common modules registered. |

### Default producer configuration

The following properties are applied automatically if not explicitly set:

| Property | Default value |
|---|---|
| `acks` | `all` |
| `retries` | `Integer.MAX_VALUE` |
| `enable.idempotence` | `true` |
| `compression.type` | `zstd` |

## Sending Commands

```java
// Async — returns a CompletableFuture
CompletableFuture<CreateCustomer> future = gateway.send(
    CreateCustomer.builder().id("customer-1").firstName("John").lastName("Doe").build()
);

// Blocking — waits up to 1 minute by default
CreateCustomer result = gateway.sendAndWait(
    CreateCustomer.builder().id("customer-1").firstName("John").lastName("Doe").build()
);

// Blocking with a custom timeout
CreateCustomer result = gateway.sendAndWait(
    CreateCustomer.builder().id("customer-1").firstName("John").lastName("Doe").build(),
    30, TimeUnit.SECONDS);
```

If the command fails, `sendAndWait` throws a `CommandExecutionException` containing the failure message. When using `send`, the returned future completes exceptionally with the same exception. If no reply is received within 5 minutes, the future completes exceptionally with a `TimeoutException`.
