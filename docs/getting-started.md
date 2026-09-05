# Getting Started

## Installation

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

## Configuration

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

Each handler class is a plain Java object. Eventify inspects each object for annotated methods and registers them automatically. You can register as many handler classes as your application requires.

### Builder options

| Method | Required | Description |
|---|---|---|
| `streamsConfig(Properties)` | Yes | Kafka Streams configuration. |
| `registerHandler(Object)` | At least one | Registers a handler class containing annotated methods. |
| `objectMapper(ObjectMapper)` | No | Custom Jackson `ObjectMapper`. Defaults to an enhanced mapper with common modules registered. |
| `stateListener(StateListener)` | No | Callback invoked on Kafka Streams state transitions. Defaults to a log statement. |
| `stateRestoreListener(StateRestoreListener)` | No | Callback invoked during state store restoration. Defaults to a logging implementation. |
| `uncaughtExceptionHandler(StreamsUncaughtExceptionHandler)` | No | Handler for uncaught stream thread exceptions. Defaults to `SHUTDOWN_CLIENT`. |

### Default Streams configuration

The following properties are applied automatically if not explicitly set in your `streamsConfig`:

| Property | Default value |
|---|---|
| `processing.guarantee` | `exactly_once_v2` |
| `topology.optimization` | `all` |
| `default.deserialization.exception.handler` | `LogAndContinueExceptionHandler` |
| Producer `compression.type` | `zstd` |

> **Note:** `exactly_once_v2` requires a Kafka cluster with at least 3 brokers in production. For local development a single broker is sufficient.

## Spring Boot Integration

The Spring Boot starter auto-configures Eventify and automatically registers any Spring bean that contains handler methods.

### Declare an Eventify bean

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

### Annotate your handler classes as Spring beans

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

The starter automatically discovers Spring beans containing handler methods and registers them with Eventify. Eventify starts when the application context is ready.

> **Important:** Auto-discovery only applies to `Eventify` beans that have **no handlers pre-registered** (i.e. the builder was not called with `registerHandler(...)`). If you register handlers manually in the builder, the Spring bean post-processor will skip that `Eventify` instance entirely. Use one approach or the other, not both.
