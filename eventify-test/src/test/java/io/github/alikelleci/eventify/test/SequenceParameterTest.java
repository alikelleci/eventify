package io.github.alikelleci.eventify.test;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.annotation.EventHandler;
import io.github.alikelleci.eventify.core.handler.annotation.Sequence;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code @Sequence} through the topology: replayed and new events, and an event read by an event handler. */
@DisplayName("Sequence parameter")
class SequenceParameterTest {

  @AggregateRoot("counter")
  public record Counter(@AggregateId String id, long lastSequence) {
  }

  @Topic("commands.counter")
  public record Increment(@AggregateId String id) {
  }

  @Topic("events.counter")
  public record Incremented(@AggregateId String id) {
  }

  @Topic("commands.audit")
  public record Audit(@AggregateId String id, long sequence) {
  }

  public static class CounterHandler {
    @CommandHandler
    public Incremented handle(Increment command, Counter state) {
      return new Incremented(command.id());
    }

    @EventSourcingHandler
    public Counter handle(Incremented event, @Sequence long sequence) {
      return new Counter(event.id(), sequence);
    }
  }

  public static class AuditHandler {
    @EventHandler
    public Audit handle(Incremented event, @Sequence Long sequence) {
      return new Audit(event.id(), sequence);
    }
  }

  @Test
  @DisplayName("Should give event-sourcing handlers the sequence of past and new events")
  void eventSourcingHandler() {
    AggregateFixture.of(Counter.class, new CounterHandler())
        .given(new Incremented("c-1"), new Incremented("c-1"))
        .when(new Increment("c-1"))
        .expectEvents(new Incremented("c-1"))
        .expectState(counter -> assertThat(counter.lastSequence()).isEqualTo(3));
  }

  @Test
  @DisplayName("Should give an event handler the sequence of the event it reads")
  void eventHandler() {
    Event event = Event.builder().aggregateType("counter").payload(new Incremented("c-1")).sequence(5).build();
    EventHandlerFixture.of(new AuditHandler())
        .when(event)
        .expectCommands(new Audit("c-1", 5));
  }
}
