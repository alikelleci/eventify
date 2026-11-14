# Spring Boot

The starter registers your handler beans and starts Eventify with the application. It needs Spring Boot 4.

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-spring-boot-starter</artifactId>
    <version>x.y.z</version>
</dependency>
```

## Set it up

Expose an `Eventify` bean and make your handler classes Spring beans. The `EventifyBuilder` the starter injects already holds every bean with handler or upcaster methods.

```java
@Bean
Eventify eventify(EventifyBuilder builder) {
    return builder.streamsConfig(config).build();
}

@Component
public class OrderHandler {
    @CommandHandler
    public OrderPlaced handle(PlaceOrder command, Order state) { ... }
}
```

Handler beans can use Spring as usual: inject dependencies, or add `@Transactional`.

## Health

With Spring Boot Actuator, the health endpoint has an `eventify` entry. It is down when Eventify has stopped.

## Spring listeners

With Spring Kafka on the classpath, the starter can also deserialize Eventify events for `@KafkaListener` methods. Select its container factory:

```java
@KafkaListener(topics = "events.order", groupId = "orders-view",
               containerFactory = "eventifyListenerContainerFactory")
public void handle(OrderPlaced event, @Timestamp Instant placedAt, @Sequence long sequence) {
    ordersView.insert(event.id(), event.customer(), placedAt);
}
```

A listener method takes the same parameters as an `@EventHandler`, or the whole `Event`. Only committed events arrive; events of a class the application doesn't have are skipped.

A listener method for one event type fails on the other types on its topic. For a topic with several types, take `Event`, or put `@KafkaListener` on a class with a `@KafkaHandler` method per type and one with `isDefault = true` for the rest.

Use this when a projection should have its own lifecycle and error handling; command handling always uses `@CommandHandler`.

## Read events yourself

To read events in a projection of your own, e.g. a Kafka Streams application, inject the starter's `EventSerde` bean. It reads events with the same upcasters as Eventify.

```java
builder.stream("events.order", Consumed.with(Serdes.String(), eventSerde));
```
