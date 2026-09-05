# Testing

Eventify works with the Kafka Streams `TopologyTestDriver`, which runs the complete processing topology in memory without requiring a running Kafka broker. This makes tests fast and deterministic.

```java
class CustomerTest {

    TopologyTestDriver driver;
    TestInputTopic<String, Command> commands;
    TestOutputTopic<String, Command> results;
    TestOutputTopic<String, Event> events;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Eventify eventify = Eventify.builder()
            .streamsConfig(props)
            .registerHandler(new CustomerCommandHandler())
            .registerHandler(new CustomerEventSourcingHandler())
            .build();

        driver = new TopologyTestDriver(eventify.topology());

        commands = driver.createInputTopic(
            "commands.customer",
            new StringSerializer(), new JsonSerializer<>());

        results = driver.createOutputTopic(
            "commands.customer.results",
            new StringDeserializer(), new JsonDeserializer<>(Command.class));

        events = driver.createOutputTopic(
            "events.customer",
            new StringDeserializer(), new JsonDeserializer<>(Event.class));
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void shouldCreateCustomer() {
        Command command = Command.builder()
            .payload(CreateCustomer.builder()
                .id("customer-1")
                .firstName("John")
                .lastName("Doe")
                .build())
            .build();

        commands.pipeInput(command.getAggregateId(), command);

        List<Command> resultList = results.readValuesToList();
        assertThat(resultList).hasSize(1);
        assertThat(resultList.get(0).getMetadata().get("$result")).isEqualTo("success");

        List<Event> eventList = events.readValuesToList();
        assertThat(eventList).hasSize(1);
        assertThat(eventList.get(0).getPayload()).isInstanceOf(CustomerCreated.class);
    }

    @Test
    void shouldFailWhenCustomerAlreadyExists() {
        Command create1 = Command.builder()
            .payload(CreateCustomer.builder().id("customer-1").firstName("John").lastName("Doe").build())
            .build();
        Command create2 = Command.builder()
            .payload(CreateCustomer.builder().id("customer-1").firstName("Jane").lastName("Doe").build())
            .build();

        commands.pipeInput(create1.getAggregateId(), create1);
        commands.pipeInput(create2.getAggregateId(), create2);

        List<Command> resultList = results.readValuesToList();
        assertThat(resultList.get(0).getMetadata().get("$result")).isEqualTo("success");
        assertThat(resultList.get(1).getMetadata().get("$result")).isEqualTo("failure");
    }
}
```

## Inspecting the stores directly

You can query the event store and snapshot store directly in your tests. Event store keys use the compound format `aggregateId@timestamp`, so a range scan from `aggregateId@` to `aggregateId@~` retrieves all events for a given aggregate.

```java
KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
KeyValueStore<String, AggregateState> snapshotStore = driver.getKeyValueStore("snapshot-store");
```

## Reserved metadata keys

The following metadata keys are used internally by Eventify and are available for assertions in tests:

| Key | Description |
|---|---|
| `$correlationId` | Auto-generated UUID assigned to every command and propagated to its events. |
| `$result` | Set to `"success"` or `"failure"` on the command result. |
| `$cause` | Set to the failure message when `$result` is `"failure"`. |
| `$replyTo` | The reply topic set by the `CommandGateway`. |
