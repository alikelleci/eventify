# Eventify

Eventify is a Java event-sourcing library backed by Apache Kafka. You define commands, events and an aggregate; Eventify records events and rebuilds aggregate state when a command arrives.

## The model

- A **command** asks to change one aggregate.
- A **command handler** validates that request and returns event payloads.
- An **event** is an immutable fact.
- An **event-sourcing handler** applies an event to produce the next aggregate state.
- An **event handler** reacts to published events, for example to update a view or send a notification.

Start with [Getting Started](getting-started.md), then define your [domain model](domain-modeling.md) and [handlers](handlers.md).

## Important choices

- Keep aggregates, commands and events immutable.
- Use the aggregate id as the command key.
- Keep event-sourcing handlers deterministic and free of side effects.
- Enable snapshots only when replaying a long history becomes expensive. With `deleteEvents = true`, history before a snapshot is no longer available.

## More

- [Command Gateway](command-gateway.md): send commands from an API or application.
- [Advanced features](advanced.md): snapshots and event upcasting.
- [Testing](testing.md): test a complete topology in memory.
- [Eventify Console](console.md): inspect aggregate history in an ops UI.
