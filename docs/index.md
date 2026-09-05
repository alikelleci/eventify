# Eventify

Eventify is a **functional event-sourcing framework** for the JVM. You define your domain logic using plain, annotated Java methods—no base classes to extend and no framework interfaces to implement.

Eventify handles event storage, state reconstruction, message routing, and event publishing. It is built entirely on Apache Kafka and Kafka Streams: commands and events flow through Kafka topics, while events are durably stored locally.

A Kafka broker is the only infrastructure you need.

## Core Concepts

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
