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
CompletableFuture<List<Event>> result = gateway.send(
    new PlaceOrder("order-1", "Ada")
);

List<Event> completed = gateway.sendAndWait(
    new PlaceOrder("order-1", "Ada")
);
```

`send` is asynchronous. `sendAndWait` blocks until a result or timeout (one minute by default). A rejected command completes exceptionally with `CommandExecutionException`. A timeout means no result arrived in time; the command may still have been handled.

## Schedule a command

```java
ScheduleToken token = gateway.schedule(new CancelOrderIfUnpaid("order-1"), Instant.now().plus(Duration.ofMinutes(30))).join();

gateway.cancel(token);
```

The command is handled at that time, not earlier. The future completes once the command is scheduled, not when it is handled. Keep the token to cancel the command; a command that is already handled stays handled, so let its handler check the state too, for example whether the order is already paid.

## Metadata

Wrap a payload in `Command` when you want to add metadata.

```java
gateway.send(Command.builder()
    .payload(new PlaceOrder("order-1", "Ada"))
    .metadata(Metadata.of("tenant", "acme"))
    .build());
```

Eventify passes command metadata to produced events and adds correlation and causation ids.

Close the gateway when the application shuts down; it implements `AutoCloseable`.
