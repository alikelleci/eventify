package io.github.alikelleci.eventify.core.store;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.support.InMemoryStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Event store")
class EventStoreTest {

  private final EventStore store = new EventStore(new InMemoryStore<Event>());

  @Test
  @DisplayName("Should preserve newest-first bounds in a validation error")
  void preservesNewestFirstBoundsInValidationError() {
    assertThatThrownBy(() -> store.eventsNewestFirst("order", "order-1", 10, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("from sequence 10 to sequence 0");
  }
}
