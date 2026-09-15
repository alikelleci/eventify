# Eventify

[![Maven Central](https://img.shields.io/maven-central/v/io.github.alikelleci/eventify-core.svg)](https://central.sonatype.com/artifact/io.github.alikelleci/eventify-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Eventify is a lightweight Java library for event sourcing built on Kafka. Add it to your Java app and get complete event sourcing out of the box—all you need is Kafka and your business logic.

Define your domain logic using plain, annotated Java methods. No base classes to extend, no framework interfaces to implement. Eventify handles event storage, state reconstruction, and event publishing for you.

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

## Eventify Console

[Eventify Console](console.md) is a web interface for your Eventify applications. Browse the event history of any aggregate, see its state after every event, and trace commands to the events they produced, including why a command failed. It runs as a Docker container, and your applications connect to it.

## Next steps

- [Getting Started](getting-started.md): add Eventify to your project and start your first application.
- [Domain Modeling](domain-modeling.md): define aggregates, commands, and events.
- [Handlers](handlers.md): write the business logic.
