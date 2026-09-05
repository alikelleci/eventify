# Domain Modeling

## Defining an Aggregate

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

## Commands and Events

Commands and events are plain, immutable value objects. The recommended pattern is to group them under a marker interface annotated with `@TopicInfo`, which declares the Kafka topic used for those messages. Every command and event class must contain exactly one `String` field annotated with `@AggregateId`. This field identifies the target aggregate instance.

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

> Bean Validation annotations such as `@NotBlank` and `@Max` on command fields are enforced automatically before the handler is invoked. If validation fails, Eventify produces a command failure result without invoking the handler.

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
