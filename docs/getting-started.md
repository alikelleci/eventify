# Getting Started

Add Eventify, define your domain, register handlers and start the application.

## Add the dependency

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-core</artifactId>
    <version>x.y.z</version>
</dependency>
```

For Spring Boot, use `eventify-spring-boot-starter` instead.

## Define a small domain

```java
@Topic("commands.order")
public record PlaceOrder(@AggregateId String id, String customer) {}

@Topic("events.order")
public record OrderPlaced(@AggregateId String id, String customer) {}

@AggregateRoot("order")
public record Order(@AggregateId String id, String customer) {}
```

## Add the behaviour

```java
public class OrderHandler {
    @HandleCommand
    public OrderPlaced handle(PlaceOrder command, Order state) {
        if (state != null) throw new ValidationException("Order already exists");
        return new OrderPlaced(command.id(), command.customer());
    }

    @ApplyEvent
    public Order apply(OrderPlaced event, Order state) {
        return new Order(event.id(), event.customer());
    }
}
```

The command handler decides which event to record. The `@ApplyEvent` method is the only place that changes aggregate state.

## Start Eventify

```java
Properties config = new Properties();
config.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders");
config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

Eventify eventify = Eventify.builder()
    .streamsConfig(config)
    .registerHandler(new OrderHandler())
    .build();

eventify.start();
```

Use the aggregate id as the key when sending a command. [Command Gateway](command-gateway.md) is the simplest way to do that.

## Spring Boot

Add the starter, expose an `Eventify` bean, and make handler classes Spring beans. The starter registers those handlers and starts Eventify.

```java
@Bean
Eventify eventify(Eventify.EventifyBuilder builder) {
    return builder.streamsConfig(config).build();
}
```

## Next steps

- [Domain modeling](domain-modeling.md)
- [Handlers](handlers.md)
- [Testing](testing.md)
