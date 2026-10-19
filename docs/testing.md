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
        "commands.order.results", new StringDeserializer(), new CommandResultSerde().deserializer());
    var events = driver.createOutputTopic(
        "events.order", new StringDeserializer(), new EventSerde().deserializer());

    Command command = Command.builder().payload(new PlaceOrder("order-1", "Ada")).build();
    commands.pipeInput(command.getAggregateId(), command);

    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
    assertThat(events.readValue().getPayload()).isEqualTo(new OrderPlaced("order-1", "Ada"));
}
```

Test both successful decisions and rejected business rules. When helpful, inspect the emitted event topic or the `event-store` and `snapshot-store` test stores directly.
