---
title: Overview
---

<div class="docs-hero" markdown>
<p class="docs-eyebrow">Eventify documentation</p>

# Build your domain.<br>Keep every event.

<p class="docs-lead">Eventify is a Java event-sourcing library backed by Apache Kafka. You define commands, events and an aggregate; Eventify records events and rebuilds aggregate state when a command arrives.</p>

<div class="docs-actions" markdown>
[Get started <span aria-hidden="true">→</span>](getting-started.md){ .docs-button .docs-button--primary }
</div>
</div>

## Explore the guides

<div class="docs-grid">
<a class="docs-card" href="domain-modeling/">
<strong>Domain Modeling <span aria-hidden="true">→</span></strong>
<span class="docs-card-description">Define your aggregates, commands, and events.</span>
</a>
<a class="docs-card" href="handlers/">
<strong>Handlers <span aria-hidden="true">→</span></strong>
<span class="docs-card-description">Validate commands, evolve state, and react to events.</span>
</a>
<a class="docs-card" href="command-gateway/">
<strong>Command Gateway <span aria-hidden="true">→</span></strong>
<span class="docs-card-description">Send commands from an API or application.</span>
</a>
<a class="docs-card" href="testing/">
<strong>Testing <span aria-hidden="true">→</span></strong>
<span class="docs-card-description">Test a complete topology in memory.</span>
</a>
</div>

## The model

- A **command** asks to change one aggregate.
- A **command handler** validates that request and returns event payloads.
- An **event** is an immutable fact.
- An **event-sourcing handler** applies an event to produce the next aggregate state.
- An **event handler** reacts to published events, for example to update a view or send a notification.

## Important choices

- Keep aggregates, commands and events immutable.
- Use the aggregate id as the command key.
- Keep event-sourcing handlers deterministic and free of side effects.
- Enable snapshots only when replaying a long history becomes expensive. With `deleteEvents = true`, history before a snapshot is no longer available.

## Advanced features and tooling

- [Advanced features](advanced.md): snapshots and event upcasting.
- [Eventify Console](console.md): inspect aggregate history in an ops UI.
