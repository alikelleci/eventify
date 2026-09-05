# Handlers

## Command Handlers

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

### Return values

| Return type | Behavior |
|---|---|
| A single event payload | One event is recorded and published. A success result is forwarded to the results topic. |
| A `List` of event payloads | Multiple events are recorded and published. A success result is forwarded. |
| `null` | No events are produced and no result record is written to any topic. |

### Throwing exceptions

Throw any exception to signal a business-rule failure. Eventify catches the exception and produces a failure result containing its message. The failure is written to the results topic (and the reply topic if applicable). Your handler does not need to create failure responses manually.

> **`null` vs exception:** returning `null` is a silent no-op—nothing is forwarded anywhere. Throwing an exception produces a visible failure result that the caller receives. Use `null` only when you intentionally want to ignore a command without any acknowledgement.

### Injectable parameters

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

## Event Sourcing Handlers

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

If multiple handlers process the same event type and you need to control their execution order, use `@Priority`. Handlers with a higher priority value are invoked first.

```java
@HandleEvent
@Priority(10)
public void on(CustomerCreated event) {
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
