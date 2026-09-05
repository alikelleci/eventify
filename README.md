# Eventify

Eventify is a **functional event-sourcing framework** for the JVM. You define your domain logic using plain, annotated Java methods—no base classes to extend and no framework interfaces to implement.

Eventify handles event storage, state reconstruction, message routing, and event publishing. It is built entirely on Apache Kafka and Kafka Streams: commands and events flow through Kafka topics, while events are durably stored locally.

A Kafka broker is the only infrastructure you need.

---

## Table of Contents

1. [Core Concepts](#1-core-concepts)
2. [Getting Started](#2-getting-started)
    - [Installation](#21-installation)
    - [Configuration](#22-configuration)
    - [Spring Boot Integration](#23-spring-boot-integration)
        - [Declare an Eventify bean](#231-declare-an-eventify-bean)
        - [Annotate your handler classes as Spring beans](#232-annotate-your-handler-classes-as-spring-beans)
3. [Domain Modeling](#3-domain-modeling)
    - [Defining an Aggregate](#31-defining-an-aggregate)
    - [Commands and Events](#32-commands-and-events)
4. [Handlers](#4-handlers)
    - [Command Handlers](#41-command-handlers)
    - [Event Sourcing Handlers](#42-event-sourcing-handlers)
    - [Event Handlers](#43-event-handlers)
5. [Command Gateway](#5-command-gateway)
    - [Configuration](#51-configuration)
    - [Sending Commands](#52-sending-commands)
6. [Advanced Features](#6-advanced-features)
    - [Snapshotting](#61-snapshotting)
    - [Event Upcasting](#62-event-upcasting)
7. [Testing](#7-testing)
8. [Annotation Reference](#8-annotation-reference)

---

## 1. Core Concepts

Before diving in, here is a brief overview of the terminology used throughout this documentation.

**Aggregate**  
An aggregate is your domain object—it represents the current state of a business entity, such as a `Customer` or an `Order`. In Eventify, an aggregate is always a plain, immutable class. Its state is not stored as the source of truth; instead, it is reconstructed from its event history, optionally starting from a snapshot.

**Command**  
A command is an instruction to perform an action—an intent to change state, such as `CreateCustomer` or `PlaceOrder`. Commands are validated and processed by command handlers. A command either succeeds and produces one or more events, or fails with an error.

**Event**  
An event is a fact—something that has already happened, such as `CustomerCreated` or `OrderPlaced`. Events are immutable and form the source of truth from which aggregate state is reconstructed.

**Command Handler**  
A class that contains the business logic for processing commands. It receives a command and the current aggregate state, validates the command, and returns the event or events that should be recorded.

**Event-Sourcing Handler**  
A class that defines how events are applied to the current aggregate state to produce the next state. This is how an aggregate is reconstructed from its event history.

**Event Handler**  
A class that reacts to published events to perform side effects, such as updating a read model, sending a notification, or triggering a downstream process.

**Upcaster**  
A class that migrates older event data to a newer schema. As your event structure evolves, upcasters transparently transform stored event data before it is deserialized.

---

## 2. Getting Started

### 2.1 Installation

Add the core dependency to your project:

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-core</artifactId>
    <version>x.y.z</version>
</dependency>
```

For Spring Boot, use the starter instead:

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-spring-boot-starter</artifactId>
    <version>x.y.z</version>
</dependency>
```

---

### 2.2 Configuration

Create an `Eventify` instance with your Kafka configuration, register your handler classes, and call `start()`.

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerHandler(new CustomerCommandHandler())
    .registerHandler(new CustomerEventSourcingHandler())
    .registerHandler(new CustomerEventHandler())
    .build();

eventify.start();
```

Each handler class is a plain Java object. Eventify inspects each object for annotated methods and registers them automatically. You can register as many handler classes as your application requires. When using Spring Boot, see [Spring Boot Integration](#23-spring-boot-integration)—handler registration and startup are handled automatically.

---

### 2.3 Spring Boot Integration

The Spring Boot starter auto-configures Eventify and automatically registers any Spring bean that contains handler methods.

### 2.3.1 Declare an Eventify bean

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

### 2.3.2 Annotate your handler classes as Spring beans

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

That's it. The starter automatically discovers Spring beans containing handler methods and registers them with Eventify. Eventify starts when the application is ready.

---

## 3. Domain Modeling

### 3.1 Defining an Aggregate

An aggregate is a plain, immutable class annotated with `@AggregateRoot`. It represents the current state of your domain entity.

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

- `@AggregateRoot` marks the class as an aggregate. Eventify also uses it to identify the aggregate state that can be injected into handler methods.
- `@Builder(toBuilder = true)` is recommended so that event-sourcing handlers can create updated state using `state.toBuilder()...build()`.
- The class should be immutable—use Lombok `@Value` or make all fields `final`.

---

### 3.2 Commands and Events

Commands and events are plain, immutable value objects. The recommended pattern is to group them under a marker interface annotated with `@TopicInfo`, which declares the Kafka topic used for those messages. Every command and event class must contain exactly one `String` field annotated with `@AggregateId`. This field identifies the target aggregate instance.

#### Commands

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

> Bean Validation annotations such as `@NotBlank` and `@Max` on command fields are enforced automatically before the handler is invoked. If validation fails, Eventify produces a command failure result without invoking the handler.

#### Events

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

## 4. Handlers

### 4.1 Command Handlers

Create a plain class and annotate its command-handling methods with `@HandleCommand`. The first parameter is always the command payload. Eventify automatically injects the remaining parameters.

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

#### Return values

| Return type | Behavior |
|---|---|
| A single event payload | One event is recorded and published. |
| A `List` of event payloads | Multiple events are recorded and published. |
| `null` | No events are produced, and no result is forwarded. |

#### Throwing exceptions

Throw any exception to signal a business-rule failure. Eventify catches the exception and produces a failure result containing its message. Your handler does not need to create failure responses manually.

#### Injectable parameters

In addition to the command payload and aggregate state, you can declare the following injectable parameters in any order:

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
| Type annotated with `@AggregateRoot` | The current aggregate state, or `null` if the aggregate does not yet exist. |
| `Metadata` | The complete metadata map for the command. |
| `@Timestamp Instant` | The command timestamp. |
| `@MessageId String` | The unique ID of the command message. |
| `@MetadataValue("key") String` | A specific value from the metadata map. |

---

### 4.2 Event Sourcing Handlers

Create a plain class and annotate its event-sourcing methods with `@ApplyEvent`. These methods define how each event is applied to produce the next aggregate state. The first parameter is the event payload; all remaining parameters are resolved by type and can appear in any order.

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

- Always return a **new** state object—never mutate the existing one.
- Return `null` to indicate that the aggregate has been deleted. Subsequent commands will receive `null` as the aggregate state.

#### Injectable parameters

| Parameter | What is injected |
|---|---|
| Type annotated with `@AggregateRoot` | The current aggregate state, or `null` if the aggregate does not yet exist. |
| `Metadata` | The complete metadata map for the event. |
| `@Timestamp Instant` | The event timestamp. |
| `@MessageId String` | The unique ID of the event message. |
| `@MetadataValue("key") String` | A specific value from the metadata map. |

---

### 4.3 Event Handlers

Create a plain class and annotate methods with `@HandleEvent` to react to published events. Event handlers are typically used for side effects such as updating a read model, sending a notification, or triggering a downstream process.

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

#### Handler priority

If multiple handlers process the same event type and you need to control their execution order, use `@Priority`. Handlers with a higher priority value are invoked first.

```java
@HandleEvent
@Priority(10)
public void on(CustomerCreated event) {
    // invoked before handlers with lower priority
}
```

#### Injectable parameters

| Parameter | What is injected |
|---|---|
| `Metadata` | The complete metadata map for the event. |
| `@Timestamp Instant` | The event timestamp. |
| `@MessageId String` | The unique ID of the event message. |
| `@MetadataValue("key") String` | A specific value from the metadata map. |

---

## 5. Command Gateway

The `CommandGateway` is the client-side component used to send commands and receive their results. It is typically used in your API layer, such as a REST controller, to dispatch commands to Eventify and await their outcome.

### 5.1 Configuration

```java
Properties producerConfig = new Properties();
producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

CommandGateway gateway = CommandGateway.builder()
    .producerConfig(producerConfig)
    .replyTopic("my-app.replies")
    .build();
```

### 5.2 Sending Commands

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

If the command fails, `sendAndWait` throws a `CommandExecutionException` containing the failure message. When using `send`, the returned future completes exceptionally with the same exception.

---

## 6. Advanced Features

### 6.1 Snapshotting

By default, aggregate state is reconstructed by replaying its event history from the beginning. For aggregates with a long history, this can become expensive. Snapshotting improves reconstruction performance by periodically saving the current aggregate state, allowing Eventify to replay only the events that occurred after the latest snapshot.

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
| `threshold` | `500` | A snapshot is created whenever the aggregate version reaches a multiple of this value. |
| `deleteEvents` | `false` | If `true`, events before the snapshot are deleted after the snapshot is created, reducing storage usage. |

Snapshotting is transparent to your handlers—you do not need to change any handler code.

---

### 6.2 Event Upcasting

As your application evolves, the structure of your events may change. Upcasting lets you transparently migrate older stored event data to a newer schema without modifying the event store.

#### How it works

1. Annotate your event class with `@Revision(n)` to declare its current schema version.
2. Write an upcaster method for each revision that needs to be migrated and annotate it with `@Upcast(type, revision)`.
3. When an older event is read, Eventify automatically chains the required upcasters in ascending revision order.

#### Example

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

- `type` is the fully qualified class name of the event payload. For nested classes, use `$` as the separator.
- `revision` is the **source** revision—the version stored in the event store, not the target revision.
- Events without a `@Revision` annotation default to revision `1`.

---

## 7. Testing

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

You can also inspect the event store and snapshot store directly in your tests:

```java
KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
KeyValueStore<String, AggregateState> snapshotStore = driver.getKeyValueStore("snapshot-store");
```

---

## 8. Annotation Reference

| Annotation | Where | Description |
|---|---|---|
| `@TopicInfo("topic")` | Command / Event interface or class | Declares the Kafka topic. Inherited by all nested classes. |
| `@AggregateId` | Field | Marks the `String` field that identifies the aggregate. |
| `@AggregateRoot` | Class | Marks a class as an aggregate root. |
| `@EnableSnapshotting` | Aggregate class | Enables periodic snapshotting. |
| `@Revision(n)` | Event payload class | Declares the current schema revision. Defaults to `1`. |
| `@HandleCommand` | Method | Marks a command-handler method. |
| `@ApplyEvent` | Method | Marks an event-sourcing handler method. |
| `@HandleEvent` | Method | Marks an event-handler method. |
| `@Upcast(type, revision)` | Method | Marks an upcaster method for a specific event type and source revision. |
| `@Priority(n)` | `@HandleEvent` method | Controls invocation order when multiple handlers exist for the same event. Higher values run first. |
| `@Timestamp` | Method parameter | Injects the message timestamp as an `Instant`. |
| `@MessageId` | Method parameter | Injects the unique message ID as a `String`. |
| `@MetadataValue("key")` | Method parameter | Injects a specific metadata value as a `String`. |

---
