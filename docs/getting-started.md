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
    .registerHandler(new OrderCommandHandler())
    .registerHandler(new OrderEventSourcingHandler())
    .registerHandler(new OrderEventHandler())
    .build();

eventify.start();
```

Each handler class is a plain Java object. Eventify inspects each object for annotated methods and registers them automatically. You can register as many handler classes as your application requires. See [Handlers](handlers.md) for how to write them.

### Builder options

| Method | Required | Description |
|---|---|---|
| `streamsConfig(Properties)` | Yes | Kafka Streams configuration. |
| `registerHandler(Object)` | At least one | Registers a handler class containing annotated methods. |
| `registerPlugin(EventifyPlugin)` | No | Registers a plugin. See [Plugins](#plugins). |
| `objectMapper(ObjectMapper)` | No | Custom Jackson `ObjectMapper`. Defaults to an enhanced mapper with common modules registered. |
| `uncaughtExceptionHandler(StreamsUncaughtExceptionHandler)` | No | Handler for uncaught stream thread exceptions. Defaults to `SHUTDOWN_CLIENT`. |

## Plugins

A plugin runs along with Eventify. It starts and stops with it, and can follow what happens underneath. Everything a plugin can do is on the `EventifyPlugin` interface, and all of it is optional:

```java
public class MyPlugin implements EventifyPlugin {

    @Override
    public void onStart(PluginContext context) { ... }

    @Override
    public void onStop(PluginContext context) { ... }

    /** Told when Kafka Streams changes state, e.g. to REBALANCING or ERROR. */
    @Override
    public StateListener stateListener() { ... }

    /** Told about the state stores being restored, and how far they are. */
    @Override
    public StateRestoreListener stateRestoreListener() { ... }
}
```

Register it on the builder:

```java
Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerPlugin(new MyPlugin())
    .build();
```

The listeners are called on Kafka Streams' own threads, so keep them short: remember something, don't block. An exception from a plugin is logged and reaches neither Kafka Streams nor the other plugins.

Eventify registers one plugin itself, which logs the state changes and the restoration progress. [Eventify Console](console.md) is a plugin too.

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
public class OrderCommandHandler {
    @HandleCommand
    public OrderEvent handle(PlaceOrder command, Order state) { ... }
}

@Component
public class OrderEventSourcingHandler {
    @ApplyEvent
    public Order apply(OrderPlaced event, Order state) { ... }
}

@Component
public class OrderEventHandler {
    @HandleEvent
    public void on(OrderPlaced event) { ... }
}
```

The starter automatically discovers Spring beans containing handler methods and registers them with Eventify. Eventify starts when the application context is ready.

> **Important:** Auto-discovery only applies to `Eventify` beans that have **no handlers pre-registered** (i.e. the builder was not called with `registerHandler(...)`).

### Handling events with `@KafkaListener`

With `spring-kafka` on the classpath, `@KafkaListener` methods can read the event topics too, as an alternative to `@HandleEvent`. Each listener gets its own consumer group, concurrency, and Spring Kafka error handling (retries, dead-letter topics). An exception in a listener doesn't stop Kafka Streams, so command handling keeps running. See [Handling events with @KafkaListener](handlers.md#handling-events-with-kafkalistener).

Commands are always handled by `@HandleCommand`.

## Next steps

- [Domain Modeling](domain-modeling.md): define your aggregates, commands, and events.
- [Command Gateway](command-gateway.md): send commands from your API layer.
- [Eventify Console](console.md): inspect your running applications.
