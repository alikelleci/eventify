# Command Gateway

Use `CommandGateway` at your application boundary to send a command and receive its outcome.

```java
CommandGateway gateway = CommandGateway.builder()
    .producerConfig(producerConfig)
    .replyTopic("orders.replies")
    .build();
```

## Send a command

```java
CompletableFuture<CommandResult.Success> result = gateway.send(
    new PlaceOrder("order-1", "Ada")
);

CommandResult.Success completed = gateway.sendAndWait(
    new PlaceOrder("order-1", "Ada")
);
```

`send` is asynchronous. `sendAndWait` blocks until a result or timeout. A rejected command completes exceptionally with `CommandExecutionException`.

## Metadata

Wrap a payload in `Command` when you want to add metadata.

```java
gateway.send(Command.builder()
    .payload(new PlaceOrder("order-1", "Ada"))
    .metadata(Metadata.of("tenant", "acme"))
    .build());
```

Eventify passes command metadata to produced events and adds correlation and causation ids.

> A timeout means the caller did not receive a result in time; it does not prove that the command was not handled.
