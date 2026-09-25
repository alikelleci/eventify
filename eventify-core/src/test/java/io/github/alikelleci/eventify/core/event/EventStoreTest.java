package io.github.alikelleci.eventify.core.event;

import io.github.alikelleci.eventify.core.support.InMemoryStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Event store")
class EventStoreTest {

  private final EventStore store = new EventStore(new InMemoryStore<Event>());

  @Test
  @DisplayName("Should refuse a sequence below 1, and name the requested range")
  void aSequenceBelowOneIsRefusedWithTheRange() {
    assertThatThrownBy(() -> store.eventsNewestFirst("order", "order-1", 10, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid range 10..0");
  }
}
