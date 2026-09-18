# Handlers

## Command Handlers

Create a plain class and annotate its command-handling methods with `@HandleCommand`. The first parameter is always the command payload. Eventify automatically injects the remaining parameters.

```java
public class OrderCommandHandler {

    @HandleCommand
    public OrderEvent handle(PlaceOrder command, Order state) {
        if (state != null) {
            throw new ValidationException("Order already exists.");
        }
        return OrderPlaced.builder()
            .id(command.getId())
            .customer(command.getCustomer())
            .build();
    }

    @HandleCommand
    public OrderEvent handle(ShipOrder command, Order state) {
        if (state == null) {
            throw new ValidationException("Order does not exist.");
        }
        return OrderShipped.builder()
            .id(command.getId())
            .trackingNumber(command.getTrackingNumber())
            .build();
    }

    @HandleCommand
    public OrderEvent handle(CancelOrder command, Order state) {
        if (state == null) {
            throw new ValidationException("Order does not exist.");
        }
        return OrderCancelled.builder()
            .id(command.getId())
            .build();
    }
}
```

### Return values

| Return type | Behavior |
|---|---|
| A single event payload | One event is recorded and published. |
| A `List` of event payloads | Multiple events are recorded and published. |
| `null` or an empty `List` | The command is accepted without events, e.g. a command that changes nothing because the aggregate is already in that state. A success result is forwarded, so the sender gets its answer. |

### Throwing exceptions

Throw any exception to signal a business-rule failure. Eventify catches the exception and produces a failure result containing its message. Your handler does not need to create failure responses manually.

### Injectable parameters

In addition to the command payload and aggregate state, you can declare the following injectable parameters in any order:

```java
@HandleCommand
public OrderEvent handle(PlaceOrder command,
                          Order state,
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

## Event Sourcing Handlers

Create a plain class and annotate its event-sourcing methods with `@ApplyEvent`. These methods define how each event is applied to produce the next aggregate state. The first parameter is the event payload; all remaining parameters are resolved by type and can appear in any order.

```java
public class OrderEventSourcingHandler {

    @ApplyEvent
    public Order apply(OrderPlaced event, Order state, @Timestamp Instant timestamp) {
        return Order.builder()
            .id(event.getId())
            .customer(event.getCustomer())
            .placedAt(timestamp) // from the event, never Instant.now(): see below
            .build();
    }

    @ApplyEvent
    public Order apply(OrderShipped event, Order state) {
        return state.toBuilder()
            .trackingNumber(event.getTrackingNumber())
            .build();
    }

    @ApplyEvent
    public Order apply(OrderCancelled event, Order state) {
        return null; // returning null signals the aggregate no longer exists
    }
}
```

- Always return a **new** state object—never mutate the existing one.
- Return `null` to indicate that the aggregate has been deleted. Subsequent commands will receive `null` as the aggregate state.
- An event without an `@ApplyEvent` method leaves the state as it was, the same as a method that returns `state`. It still counts for the aggregate's version, e.g. a failure event that only a saga reacts to.
- Keep these methods **deterministic and free of side effects**. They run again every time the aggregate is loaded, and whenever the Eventify Console shows its history, so the same events must always give the same state. Use only the event, the state and the injected parameters: no `Instant.now()`, random values, database lookups or calls to other services.

### Injectable parameters

| Parameter | What is injected |
|---|---|
| Type annotated with `@AggregateRoot` | The current aggregate state, or `null` if the aggregate does not yet exist. |
| `Metadata` | The complete metadata map for the event. |
| `@Timestamp Instant` | The event timestamp. |
| `@MessageId String` | The unique ID of the event message. |
| `@MetadataValue("key") String` | A specific value from the metadata map. |

## Event Handlers

Create a plain class and annotate methods with `@HandleEvent` to react to published events. Event handlers are typically used for side effects such as updating a read model, sending a notification, or triggering a downstream process.

```java
public class OrderEventHandler {

    @HandleEvent
    public void on(OrderPlaced event) {
        // e.g. insert into a read model database
    }

    @HandleEvent
    public void on(OrderShipped event) {
        // e.g. update the read model
    }

    @HandleEvent
    public void on(OrderCancelled event) {
        // e.g. remove from the read model
    }
}
```

### Handler priority

If multiple handlers process the same event type and you need to control their execution order, use `@Priority`. Handlers with a higher priority value are invoked first.

```java
@HandleEvent
@Priority(10)
public void on(OrderPlaced event) {
    // invoked before handlers with lower priority
}
```

### Injectable parameters

| Parameter | What is injected |
|---|---|
| `Metadata` | The complete metadata map for the event. |
| `@Timestamp Instant` | The event timestamp. |
| `@MessageId String` | The unique ID of the event message. |
| `@MetadataValue("key") String` | A specific value from the metadata map. |

### Reading event topics outside Eventify

A consumer that reads the event topics without Eventify, for example a projection of your own, must set `isolation.level` to `read_committed`. With Kafka's default, `read_uncommitted`, it also receives events of commands that failed and were rolled back: events of things that never happened.

```java
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
```

### Run event handlers in a separate application

An exception thrown by an event handler stops the Eventify instance it runs in. This is on purpose: the event is not skipped, so after you fix the problem and restart, the event is handled again and nothing is lost.

When that instance also handles commands, command handling stops with it. Run your event handlers in a separate application, with its own `application.id`, so a failing event handler never stops command handling.

## Handling events with @KafkaListener

With the Spring Boot starter and `spring-kafka` on the classpath, you can read Eventify's event topics with Spring Kafka's `@KafkaListener` instead of `@HandleEvent`. Set `containerFactory = "eventifyListenerContainerFactory"`: the listener container factory the starter provides for Eventify events.

### All events of a topic

An event topic usually has all events of an aggregate: `OrderPlaced`, `OrderShipped`, `OrderCancelled`, ... Put `@KafkaListener` on the class and `@KafkaHandler` on a method per event type. Spring Kafka calls the method for the payload's type, as Eventify does for `@HandleEvent`:

```java
@Component
@KafkaListener(topics = "orders.events", groupId = "order-projection",
               containerFactory = "eventifyListenerContainerFactory")
public class OrderProjection {

    @KafkaHandler
    public void on(OrderPlaced event, Metadata metadata) {
        // e.g. insert into a read model database
    }

    @KafkaHandler
    public void on(OrderShipped event, @Timestamp Instant timestamp) { ... }

    @KafkaHandler
    public void on(OrderCancelled event) { ... }

    @KafkaHandler(isDefault = true)
    public void ignore(Object event) {
        // the other events of the topic
    }
}
```

Unlike `@HandleEvent`, an event type without a method is an error, handled by the error handler (see below). Add the `isDefault` method to ignore the types you don't handle.

To also get the whole `Event`, add it as a parameter next to the payload. The `isDefault` method can take the `Event` alone:

```java
    @KafkaHandler
    public void on(OrderPlaced event, Event whole) { ... }

    @KafkaHandler(isDefault = true)
    public void other(Event whole) { ... }
```

A `@KafkaHandler` method with only an `Event` parameter, and not `isDefault`, is never called: Spring Kafka picks the method by the payload's type, and a payload is never an `Event`.

### A listener method

With `@KafkaListener` on a method, every event of the topic goes to that method. Use it for a topic with one event type, or to get all events in one method:

```java
@KafkaListener(topics = "payments.events", groupId = "payment-projection",
               containerFactory = "eventifyListenerContainerFactory")
public void on(PaymentReceived event, Metadata metadata) { ... }   // the topic has only PaymentReceived

@KafkaListener(topics = "orders.events", groupId = "audit-log",
               containerFactory = "eventifyListenerContainerFactory")
public void on(Event event) { ... }                                // all events, e.g. for an audit log
```

A method that takes one payload type, on a topic with other types too, fails for those other events.

### Injectable parameters

The payload comes first, as with `@HandleEvent`. Then any of:

| Parameter | What is injected |
|---|---|
| `Event` | The whole event: payload, id, timestamp, metadata, aggregate id. |
| `Metadata` | The complete metadata map for the event. |
| `@Timestamp Instant` | The event timestamp. |
| `@MessageId String` | The unique ID of the event message. |
| `@MetadataValue("key") String` | A specific value from the metadata map. |

Spring Kafka's own parameters work too, e.g. `@Header(KafkaHeaders.RECEIVED_PARTITION) int partition`.

### Why use @KafkaListener

- **Failures stay in the listener.** An exception is handled by Spring Kafka's error handler: retries, then the next record or a [dead-letter topic](#errors-and-dead-letter-topics). Kafka Streams and command handling keep running.
- **One consumer group per listener**, so each projection keeps its own offsets and can be reset or replayed on its own.
- **Concurrency per listener**, with `@KafkaListener(concurrency = "3")`.

### What the starter sets up

- Events are read as Eventify writes them, with the `ObjectMapper` of your `Eventify` bean. They are upcasted with the same upcasters as your `Eventify` bean uses, also those registered with `registerHandler(...)`. Without an `Eventify` bean, the `@Upcast` methods of your beans are used.
- `isolation.level` is always `read_committed`, so events of commands that were rolled back are never delivered. `auto.offset.reset` is `earliest` unless you set it, so a new projection starts from the first event.
- The connection settings come from your `Eventify` bean's `streamsConfig`: `bootstrap.servers`, security settings, and `consumer.`-prefixed settings. Without an `Eventify` bean, they come from Spring Kafka's consumer factory (`spring.kafka.*`). An application with only listeners doesn't need an `Eventify` bean.

### Errors and dead-letter topics

By default, Spring Kafka retries a failed event 9 times, logs it, and goes on with the next one: the event is lost for that listener. To keep failed events, declare a `CommonErrorHandler` bean: the starter's factory uses it. For example, a `DefaultErrorHandler` that sends them to a dead-letter topic (`<topic>-dlt`):

```java
@Bean
DefaultErrorHandler errorHandler(Eventify eventify) {
    Map<Class<?>, Serializer<?>> serializers = new LinkedHashMap<>();
    serializers.put(byte[].class, new ByteArraySerializer());                    // records that could not be read
    serializers.put(Event.class, new JsonSerializer<>(eventify.getObjectMapper())); // Eventify's JsonSerializer

    KafkaTemplate<String, Object> template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
        new StringSerializer(), new DelegatingByTypeSerializer(serializers)));

    return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template),
                                   new FixedBackOff(1000, 3));  // 3 retries, 1 second apart
}
```

Write the events with Eventify's `JsonSerializer`, as above. The dead-letter topic then has the events as Eventify wrote them, and you can read it with a listener on `containerFactory = "eventifyListenerContainerFactory"`, e.g. to handle them again once the problem is fixed. With another serializer, such as Spring Kafka's `JsonSerializer`, the events are written in another format and can't be read as events.

A record that could not be read at all, e.g. one that is not JSON, has no event: `ByteArraySerializer` writes its bytes as they were.

### Commands

Commands can't be handled with `@KafkaListener`. Handling a command needs Eventify's event store, and Eventify writes the events, the result, and the event store in one transaction. Commands are always handled by `@HandleCommand`.

## Thread safety

One handler object is used by all stream threads (`num.stream.threads`) and by the Eventify Console at the same time. Keep handlers stateless: only `final` dependencies such as repositories or clients, and no fields that change.
