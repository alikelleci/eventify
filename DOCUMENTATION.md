# Eventify

Eventify is a **functional event sourcing framework** for the JVM. It lets you build event-driven applications using plain Java classes and annotations — no base classes to extend, no framework interfaces to implement.

You define your domain logic as simple annotated methods. Eventify takes care of storing events, rebuilding aggregate state, routing messages, and publishing results.

---

## Table of Contents

1. [Core Concepts](#1-core-concepts)
2. [Installation](#2-installation)
3. [Defining Commands and Events](#3-defining-commands-and-events)
4. [Defining the Aggregate](#4-defining-the-aggregate)
5. [Handling Commands](#5-handling-commands)
6. [Rebuilding State with Event Sourcing](#6-rebuilding-state-with-event-sourcing)
7. [Handling Events](#7-handling-events)
8. [Upcasting](#8-upcasting)
9. [Snapshotting](#9-snapshotting)
10. [Sending Commands with the Command Gateway](#10-sending-commands-with-the-command-gateway)
11. [Spring Boot Integration](#11-spring-boot-integration)
12. [Testing](#12-testing)
13. [Annotations Quick Reference](#13-annotations-quick-reference)

---

## 1. Core Concepts

Before diving in, here is a brief overview of the terminology used throughout this documentation.

**Aggregate**
The aggregate is your domain object — it represents the current state of a business entity (e.g. a `Customer`, an `Order`). In Eventify, the aggregate is always a plain immutable class. Its state is never stored directly; instead it is rebuilt on demand by replaying the events that happened to it.

**Command**
A command is an instruction to do something — an intent to change state (e.g. `CreateCustomer`, `PlaceOrder`). Commands are validated and processed by a command handler. A command either succeeds and produces one or more events, or fails with an error.

**Event**
An event is a fact — something that has already happened (e.g. `CustomerCreated`, `OrderPlaced`). Events are immutable and stored permanently. They are the source of truth for rebuilding aggregate state.

**Command Handler**
A class that contains the business logic for processing commands. It receives a command and the current aggregate state, validates the command, and returns the event(s) that should be recorded.

**Event Sourcing Handler**
A class that knows how to apply an event to the current aggregate state and return the new state. This is how the aggregate is rebuilt from its history.

**Event Handler**
A class that reacts to published events for side-effects — updating a read model, sending a notification, triggering a downstream process, etc.

**Upcaster**
A class that migrates old event data to a newer schema. When your event structure changes over time, upcasters transform the stored data transparently before it is deserialized.

---

## 2. Installation

Add the core dependency to your project:

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-core</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

For Spring Boot, use the starter instead:

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-spring-boot-starter</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

---

## 3. Defining Commands and Events

Commands and events are plain immutable value objects. The recommended pattern is to group them under a marker interface annotated with `@TopicInfo`, which declares the Kafka topic they belong to.

Every command and event class must have exactly one `String` field annotated with `@AggregateId` — this is the identifier of the aggregate they belong to.

### Commands

```java
@TopicInfo("commands.customer")
public interface CustomerCommand {

    @Value
    @Builder
    class CreateCustomer implements CustomerCommand {
        @AggregateId
        String id;
        @NotBlank
        String firstName;
        @NotBlank
        String lastName;
    }

    @Value
    @Builder
    class ChangeFirstName implements CustomerCommand {
        @AggregateId
        String id;
        @NotBlank
        String firstName;
    }

    @Value
    @Builder
    class DeleteCustomer implements CustomerCommand {
        @AggregateId
        String id;
    }
}
```

> Bean Validation annotations (e.g. `@NotBlank`, `@Max`) on command fields are automatically enforced before the handler is invoked. A validation failure produces a command failure result without invoking your handler.

### Events

```java
@TopicInfo("events.customer")
public interface CustomerEvent {

    @Value
    @Builder
    class CustomerCreated implements CustomerEvent {
        @AggregateId
        String id;
        String firstName;
        String lastName;
    }

    @Value
    @Builder
    class FirstNameChanged implements CustomerEvent {
        @AggregateId
        String id;
        String firstName;
    }

    @Value
    @Builder
    class CustomerDeleted implements CustomerEvent {
        @AggregateId
        String id;
    }
}
```

---

## 4. Defining the Aggregate

The aggregate is a plain immutable class annotated with `@AggregateRoot`. It holds the current state of your domain entity.

```java
@Value
@Builder(toBuilder = true)
@AggregateRoot
public class Customer {
    @AggregateId
    String id;
    String firstName;
    String lastName;
    Instant createdAt;
}
```

- `@AggregateRoot` marks the class as an aggregate. It is also used by the framework to inject the current state into handler methods.
- `@Builder(toBuilder = true)` is recommended so that event sourcing handlers can produce updated state using `state.toBuilder()...build()`.
- The class should be immutable — use Lombok `@Value` or make all fields `final`.

---

## 5. Handling Commands

Create a plain class and annotate methods with `@HandleCommand`. The first parameter is always the command payload. The framework injects the remaining parameters automatically.

```java
public class CustomerCommandHandler {

    @HandleCommand
    public CustomerEvent handle(CreateCustomer command, Customer state) {
        if (state != null) {
            throw new ValidationException("Customer already exists.");
        }
        return CustomerCreated.builder()
            .id(command.getId())
            .firstName(command.getFirstName())
            .lastName(command.getLastName())
            .build();
    }

    @HandleCommand
    public CustomerEvent handle(ChangeFirstName command, Customer state) {
        if (state == null) {
            throw new ValidationException("Customer does not exist.");
        }
        return FirstNameChanged.builder()
            .id(command.getId())
            .firstName(command.getFirstName())
            .build();
    }

    @HandleCommand
    public CustomerEvent handle(DeleteCustomer command, Customer state) {
        if (state == null) {
            throw new ValidationException("Customer does not exist.");
        }
        return CustomerDeleted.builder()
            .id(command.getId())
            .build();
    }
}
```

### Return values

| Return type | Behaviour |
|---|---|
| A single event payload | One event is recorded and published. |
| A `List` of event payloads | Multiple events are recorded and published. |
| `null` | No events are produced and no result is forwarded. |

### Throwing exceptions

Throw any exception to signal a business rule failure. The framework catches it and produces a failure result containing the error message. Your handler is never responsible for producing failure responses manually.

### Injectable parameters

Beyond the command payload and the aggregate state, you can declare additional parameters in any order:

```java
@HandleCommand
public CustomerEvent handle(CreateCustomer command,
                            Customer state,
                            Metadata metadata,
                            @Timestamp Instant timestamp,
                            @MessageId String messageId,
                            @MetadataValue("$correlationId") String correlationId) {
    // ...
}
```

| Parameter | What is injected |
|---|---|
| Type annotated with `@AggregateRoot` | The current aggregate state, or `null` if the aggregate does not exist yet. |
| `Metadata` | The full metadata map of the command. |
| `@Timestamp Instant` | The timestamp of the command. |
| `@MessageId String` | The unique ID of the command message. |
| `@MetadataValue("key") String` | A specific value from the metadata map. |

---

## 6. Rebuilding State with Event Sourcing

Create a plain class and annotate methods with `@ApplyEvent`. These methods define how each event transforms the aggregate state. The first parameter is the event payload; the second is the current state.

```java
public class CustomerEventSourcingHandler {

    @ApplyEvent
    public Customer apply(CustomerCreated event, Customer state) {
        return Customer.builder()
            .id(event.getId())
            .firstName(event.getFirstName())
            .lastName(event.getLastName())
            .createdAt(Instant.now())
            .build();
    }

    @ApplyEvent
    public Customer apply(FirstNameChanged event, Customer state) {
        return state.toBuilder()
            .firstName(event.getFirstName())
            .build();
    }

    @ApplyEvent
    public Customer apply(CustomerDeleted event, Customer state) {
        return null; // returning null signals the aggregate no longer exists
    }
}
```

- Always return a **new** state object — never mutate the existing one.
- Return `null` to signal that the aggregate has been deleted. Subsequent commands will receive `null` as the state.
- The same additional parameters available in command handlers (`@Timestamp`, `@MessageId`, `Metadata`, `@MetadataValue`) can be injected here too.

---

## 7. Handling Events

Create a plain class and annotate methods with `@HandleEvent` to react to published events. This is where you implement side-effects such as updating a read model or sending a notification.

```java
public class CustomerEventHandler {

    @HandleEvent
    public void on(CustomerCreated event) {
        // e.g. insert into a read model database
    }

    @HandleEvent
    public void on(FirstNameChanged event) {
        // e.g. update the read model
    }

    @HandleEvent
    public void on(CustomerDeleted event) {
        // e.g. remove from the read model
    }
}
```

### Handler priority

If you have multiple handlers for the same event type and need to control their execution order, use `@Priority`. Handlers with a higher value are invoked first.

```java
@HandleEvent
@Priority(10)
public void on(CustomerCreated event) {
    // invoked before handlers with lower priority
}
```

The same injectable parameters (`Metadata`, `@Timestamp`, `@MessageId`, `@MetadataValue`) are available here as well.

---

## 8. Upcasting

As your application evolves, the structure of your events may change. Upcasting lets you migrate old stored events to a newer schema transparently, without touching the event store.

### How it works

1. Annotate your event class with `@Revision(n)` to declare its current schema version.
2. Write an upcaster method for each revision that needs to be migrated, annotated with `@Upcast(type, revision)`.
3. Upcasters are chained automatically in ascending revision order when an old event is read.

### Example

Suppose `CustomerCreated` started at revision 1 and is now at revision 3 after two schema changes:

```java
// Current version of the event — revision 3
@Revision(3)
@Value
@Builder
class CustomerCreated implements CustomerEvent {
    @AggregateId
    String id;
    String firstName;
    String lastName;
    String email;       // added in revision 2
    String phoneNumber; // added in revision 3
}
```

```java
public class CustomerEventUpcaster {

    // Migrates revision 1 → 2: adds a default email
    @Upcast(type = "com.example.CustomerEvent$CustomerCreated", revision = 1)
    public JsonNode upcast(ObjectNode node) {
        node.put("email", "unknown@example.com");
        return node;
    }

    // Migrates revision 2 → 3: adds a default phone number
    @Upcast(type = "com.example.CustomerEvent$CustomerCreated", revision = 2)
    public JsonNode upcast(ObjectNode node) {
        node.put("phoneNumber", "unknown");
        return node;
    }
}
```

- `type` is the fully qualified class name of the event payload. For nested classes, use `$` as the separator (e.g. `com.example.CustomerEvent$CustomerCreated`).
- `revision` is the **source** revision — the version of the event as it is stored, not the target.
- If an event has no `@Revision` annotation, it defaults to revision `1`.

---

## 9. Snapshotting

By default, aggregate state is rebuilt by replaying all events from the beginning. For aggregates with a long history, this can become slow. Snapshotting solves this by periodically saving the current state so that only events after the last snapshot need to be replayed.

Enable snapshotting by adding `@EnableSnapshotting` to your aggregate class:

```java
@Value
@Builder(toBuilder = true)
@AggregateRoot
@EnableSnapshotting(threshold = 500)
public class Customer {
    // ...
}
```

| Attribute | Default | Description |
|---|---|---|
| `threshold` | `500` | A snapshot is saved every time the aggregate version is a multiple of this number. |
| `deleteEvents` | `false` | If `true`, events prior to the snapshot are deleted after the snapshot is saved, reducing storage usage. |

Snapshotting is completely transparent — you do not need to change any handler code.

---

## 10. Sending Commands with the Command Gateway

The `CommandGateway` is the client-side entry point for sending commands and receiving results. It sends the command to Kafka and waits for the result asynchronously.

### Setup

```java
Properties producerConfig = new Properties();
producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

CommandGateway gateway = CommandGateway.builder()
    .producerConfig(producerConfig)
    .replyTopic("my-app.replies") // a dedicated single-partition topic for results
    .build();
```

### Sending commands

```java
// Async — returns a CompletableFuture
CompletableFuture<CustomerCreated> future = gateway.send(
    CreateCustomer.builder().id("customer-1").firstName("John").lastName("Doe").build()
);

// Blocking — waits up to 1 minute by default
CustomerCreated result = gateway.sendAndWait(
    CreateCustomer.builder().id("customer-1").firstName("John").lastName("Doe").build()
);

// Blocking with a custom timeout
CustomerCreated result = gateway.sendAndWait(command, 30, TimeUnit.SECONDS);
```

If the command fails, `sendAndWait` throws a `CommandExecutionException` with the failure message. With `send`, the future completes exceptionally with the same exception.

> The reply topic must exist in Kafka before the gateway is started and should have exactly **one partition**.

---

## 11. Spring Boot Integration

The Spring Boot starter auto-configures Eventify and automatically registers any Spring bean that contains handler methods.

### 1. Declare an Eventify bean

```java
@Configuration
public class EventifyConfig {

    @Bean
    public Eventify eventify() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        return Eventify.builder()
            .streamsConfig(props)
            .build();
    }
}
```

### 2. Annotate your handler classes as Spring beans

```java
@Component
public class CustomerCommandHandler {
    @HandleCommand
    public CustomerEvent handle(CreateCustomer command, Customer state) { ... }
}

@Component
public class CustomerEventSourcingHandler {
    @ApplyEvent
    public Customer apply(CustomerCreated event, Customer state) { ... }
}

@Component
public class CustomerEventHandler {
    @HandleEvent
    public void on(CustomerCreated event) { ... }
}
```

That's it. The starter detects all beans with handler methods and registers them automatically. Eventify starts when the application is ready.

---

## 12. Testing

Eventify works with the Kafka Streams `TopologyTestDriver`, which runs the entire processing pipeline in-memory without a running Kafka broker. This makes tests fast and deterministic.

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

You can also inspect the event store and snapshot store directly during tests:

```java
KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
KeyValueStore<String, AggregateState> snapshotStore = driver.getKeyValueStore("snapshot-store");
```

---

## 13. Annotations Quick Reference

| Annotation | Where | Description |
|---|---|---|
| `@TopicInfo("topic")` | Command / Event interface or class | Declares the Kafka topic. Inherited by all nested classes. |
| `@AggregateId` | Field | Marks the `String` field that identifies the aggregate. |
| `@AggregateRoot` | Class | Marks a class as an aggregate root. |
| `@EnableSnapshotting` | Aggregate class | Enables periodic snapshotting. |
| `@Revision(n)` | Event payload class | Declares the current schema revision. Defaults to `1`. |
| `@HandleCommand` | Method | Marks a command handler method. |
| `@ApplyEvent` | Method | Marks an event sourcing handler method. |
| `@HandleEvent` | Method | Marks an event handler method. |
| `@Upcast(type, revision)` | Method | Marks an upcaster method for a specific event type and source revision. |
| `@Priority(n)` | `@HandleEvent` method | Controls invocation order when multiple handlers exist for the same event. Higher = first. |
| `@Timestamp` | Method parameter | Injects the message timestamp as `Instant`. |
| `@MessageId` | Method parameter | Injects the unique message ID as `String`. |
| `@MetadataValue("key")` | Method parameter | Injects a specific metadata value as `String`. |
