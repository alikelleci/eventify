package io.github.alikelleci.eventify.core.handler;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.internal.EventSourcingHandlerMethod;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.internal.EventHandlerMethod;
import io.github.alikelleci.eventify.core.handler.annotation.Sequence;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Event handlers and event sourcing handlers get the event's sequence for {@code @Sequence}. */
@DisplayName("Sequence parameter")
class SequenceParameterTest {

  record OrderPlaced(@AggregateId String id) {
  }

  @AggregateRoot("order")
  record Order(@AggregateId String id, long placedAt) {
  }

  static class Handlers {
    Object handled;

    void on(OrderPlaced event, @Sequence Long sequence) {
      handled = sequence;
    }

    Order apply(OrderPlaced event, Order state, @Sequence long sequence) {
      return new Order(event.id(), sequence);
    }
  }

  private final Event event = Event.builder().aggregateType("order").payload(new OrderPlaced("order-1")).sequence(4).build();

  @Test
  @DisplayName("Should give an event handler the event's sequence")
  void eventHandler() {
    Handlers handlers = new Handlers();
    new EventHandlerMethod(handlers, method("on")).handle(event);
    assertThat(handlers.handled).isEqualTo(4L);
  }

  @Test
  @DisplayName("Should give an event sourcing handler the event's sequence")
  void eventSourcingHandler() {
    Object state = new EventSourcingHandlerMethod(new Handlers(), method("apply")).handle(event, AggregateState.empty("order-1"));
    assertThat(state).isEqualTo(new Order("order-1", 4));
  }

  private static Method method(String name) {
    return Arrays.stream(Handlers.class.getDeclaredMethods()).filter(m -> m.getName().equals(name)).findFirst().orElseThrow();
  }
}
