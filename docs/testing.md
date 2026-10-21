# Testing

Use Kafka Streams' `TopologyTestDriver` to test commands, events and results without a broker.

```java
Properties testConfig = new Properties();
testConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-test");
testConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

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

Test both successful decisions and rejected business rules. Assert the command result and the events your application publishes.
