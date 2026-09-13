# Testing

Eventify works with the Kafka Streams `TopologyTestDriver`, which runs the complete processing topology in memory without requiring a running Kafka broker. This makes tests fast and deterministic.

```java
class OrderTest {

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
            .registerHandler(new OrderCommandHandler())
            .registerHandler(new OrderEventSourcingHandler())
            .build();

        driver = new TopologyTestDriver(eventify.topology());

        commands = driver.createInputTopic(
            "commands.order",
            new StringSerializer(), new JsonSerializer<>());

        results = driver.createOutputTopic(
            "commands.order.results",
            new StringDeserializer(), new JsonDeserializer<>(Command.class));

        events = driver.createOutputTopic(
            "events.order",
            new StringDeserializer(), new JsonDeserializer<>(Event.class));
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void shouldPlaceOrder() {
        Command command = Command.builder()
            .payload(PlaceOrder.builder()
                .id("order-1")
                .customer("John Doe")
                .shippingAddress("123 Main St")
                .build())
            .build();

        commands.pipeInput(command.getAggregateId(), command);

        List<Command> resultList = results.readValuesToList();
        assertThat(resultList).hasSize(1);
        assertThat(resultList.get(0).getMetadata().get("$result")).isEqualTo("success");

        List<Event> eventList = events.readValuesToList();
        assertThat(eventList).hasSize(1);
        assertThat(eventList.get(0).getPayload()).isInstanceOf(OrderPlaced.class);
    }

    @Test
    void shouldFailWhenOrderAlreadyExists() {
        Command place1 = Command.builder()
            .payload(PlaceOrder.builder().id("order-1").customer("John Doe").shippingAddress("123 Main St").build())
            .build();
        Command place2 = Command.builder()
            .payload(PlaceOrder.builder().id("order-1").customer("Jane Doe").shippingAddress("456 Oak Ave").build())
            .build();

        commands.pipeInput(place1.getAggregateId(), place1);
        commands.pipeInput(place2.getAggregateId(), place2);

        List<Command> resultList = results.readValuesToList();
        assertThat(resultList.get(0).getMetadata().get("$result")).isEqualTo("success");
        assertThat(resultList.get(1).getMetadata().get("$result")).isEqualTo("failure");
    }
}
```

## Inspecting the stores directly

You can query the event store and snapshot store directly in your tests:

```java
KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
KeyValueStore<String, AggregateState> snapshotStore = driver.getKeyValueStore("snapshot-store");
```
