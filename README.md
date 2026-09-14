# Eventify

[![CI](https://github.com/alikelleci/eventify/actions/workflows/ci.yml/badge.svg)](https://github.com/alikelleci/eventify/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.alikelleci/eventify-core.svg)](https://central.sonatype.com/artifact/io.github.alikelleci/eventify-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Eventify is a **functional event-sourcing framework** for the JVM. You define your domain logic using plain, annotated Java methods—no base classes to extend and no framework interfaces to implement.

Eventify handles event storage, state reconstruction, message routing, and event publishing. It is built entirely on Apache Kafka and Kafka Streams: commands and events flow through Kafka topics, while events are durably stored locally.

A Kafka broker is the only infrastructure you need.

**[Website](https://alikelleci.github.io/eventify/)** · **[Documentation](https://alikelleci.github.io/eventify/docs/)** · **[Eventify Console](https://alikelleci.github.io/eventify/console)**

---

## Quick start

Add the core dependency (or `eventify-spring-boot-starter` for Spring Boot):

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-core</artifactId>
    <version>x.y.z</version>
</dependency>
```

Write your domain logic as plain, annotated methods:

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
}

public class OrderEventSourcingHandler {

    @ApplyEvent
    public Order apply(OrderPlaced event, Order state) {
        return Order.builder()
            .id(event.getId())
            .customer(event.getCustomer())
            .build();
    }
}
```

Register your handlers and start:

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerHandler(new OrderCommandHandler())
    .registerHandler(new OrderEventSourcingHandler())
    .build();

eventify.start();
```

See the [Getting Started guide](https://alikelleci.github.io/eventify/docs/getting-started/) for aggregates, commands and events, and the Spring Boot integration.

---

## Documentation

| Topic | |
|---|---|
| [Getting Started](https://alikelleci.github.io/eventify/docs/getting-started/) | Installation, configuration and Spring Boot integration |
| [Domain Modeling](https://alikelleci.github.io/eventify/docs/domain-modeling/) | Aggregates, commands and events |
| [Handlers](https://alikelleci.github.io/eventify/docs/handlers/) | Command, event sourcing and event handlers |
| [Command Gateway](https://alikelleci.github.io/eventify/docs/command-gateway/) | Sending commands and receiving their results |
| [Advanced Features](https://alikelleci.github.io/eventify/docs/advanced/) | Snapshotting and event upcasting |
| [Testing](https://alikelleci.github.io/eventify/docs/testing/) | Testing without a Kafka broker |
| [Annotation Reference](https://alikelleci.github.io/eventify/docs/annotation-reference/) | All annotations at a glance |
| [Eventify Console](https://alikelleci.github.io/eventify/docs/console/) | Installation, deployment modes and security |

---

## Modules

| Module | Description |
|---|---|
| `eventify-core` | The framework |
| `eventify-spring-boot-starter` | Spring Boot auto-configuration: registers handler beans and starts Eventify with the application |
| `eventify-console-server` | Optional: embeds the console's HTTP server and API into your application |
| `eventify-console-ui` | Optional: the console UI, served by the console server |
| `ghcr.io/alikelleci/eventify-console` | Optional: the standalone console as a Docker image, for multiple applications |

---

## License

Eventify is licensed under the [Apache License, Version 2.0](LICENSE).
