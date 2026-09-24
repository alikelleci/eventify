# Testing

Use Kafka Streams' `TopologyTestDriver` to test commands, events and results without a broker.

```java
Eventify eventify = Eventify.builder()
    .streamsConfig(testConfig)
    .registerHandler(new OrderHandler())
    .build();

try (TopologyTestDriver driver = new TopologyTestDriver(eventify.topology())) {
    var commands = driver.createInputTopic(
        "commands.order", new StringSerializer(), new CommandSerde().serializer());
    var results = driver.createOutputTopic(
        "commands.order.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));

    Command command = Command.builder().payload(new PlaceOrder("order-1", "Ada")).build();
    commands.pipeInput(command.getAggregateId(), command);

    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
}
```

Test both successful decisions and rejected business rules. When helpful, inspect the emitted event topic or the `event-store` and `snapshot-store` test stores directly.
