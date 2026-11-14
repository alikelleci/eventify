---
title: Overview
hide:
  - navigation
  - toc
---

<div class="docs-hero" markdown>
<p class="docs-eyebrow">Eventify documentation</p>

# Event sourcing on Kafka, in plain Java

<p class="docs-lead">You write commands, events and handlers. Eventify stores every event in Kafka, rebuilds an aggregate's state when a command arrives and passes new events on to whoever reacts to them. No database, no extra server.</p>

<div class="docs-actions" markdown>
[Get started <span aria-hidden="true">→</span>](getting-started.md){ .docs-button .docs-button--primary }
[Spring Boot](spring-boot.md){ .docs-button }
</div>
</div>

## How it works

<div class="docs-flow" markdown>
<div class="docs-flow-step" markdown>
<span class="docs-flow-number">1</span>
**Send a command**

`PlaceOrder` goes to the aggregate's command topic, keyed by its id.
</div>
<div class="docs-flow-step" markdown>
<span class="docs-flow-number">2</span>
**Decide**

A `@CommandHandler` gets the current state and returns events, or rejects the command.
</div>
<div class="docs-flow-step" markdown>
<span class="docs-flow-number">3</span>
**Record**

The events are stored and published in one transaction. `@EventSourcingHandler` methods build the next state.
</div>
<div class="docs-flow-step" markdown>
<span class="docs-flow-number">4</span>
**React**

`@EventHandler` methods update views, send mail or return the next command.
</div>
</div>

## Guides

<div class="docs-grid">
<a class="docs-card" href="getting-started/">
<strong>Getting started <span aria-hidden="true">→</span></strong>
<span class="docs-card-description">A first aggregate, from dependency to running application.</span>
</a>
<a class="docs-card" href="domain-modeling/">
<strong>Domain modeling <span aria-hidden="true">→</span></strong>
<span class="docs-card-description">Aggregates, commands and events.</span>
</a>
<a class="docs-card" href="handlers/">
<strong>Handlers <span aria-hidden="true">→</span></strong>
<span class="docs-card-description">Decide on commands, build state and react to events.</span>
</a>
<a class="docs-card" href="command-gateway/">
<strong>Command gateway <span aria-hidden="true">→</span></strong>
<span class="docs-card-description">Send and schedule commands, and wait for their result.</span>
</a>
<a class="docs-card" href="spring-boot/">
<strong>Spring Boot <span aria-hidden="true">→</span></strong>
<span class="docs-card-description">The starter, handler beans and <code>@KafkaListener</code>.</span>
</a>
<a class="docs-card" href="testing/">
<strong>Testing <span aria-hidden="true">→</span></strong>
<span class="docs-card-description">Given, when, then: without a broker.</span>
</a>
</div>

## Good to know

- Keep aggregates, commands and events immutable.
- Use the aggregate id as the command key.
- Keep event-sourcing handlers deterministic and free of side effects.
- Enable snapshots only when replaying a long history becomes expensive. With `deleteEvents = true`, history before a snapshot is gone.
