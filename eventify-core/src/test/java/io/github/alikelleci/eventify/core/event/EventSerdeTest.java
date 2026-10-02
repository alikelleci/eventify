package io.github.alikelleci.eventify.core.event;

import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Upcasting while reading: see UpcastingChainTest. */
@DisplayName("Event serde")
class EventSerdeTest {

  @Topic("events.order")
  public record OrderPlaced(@AggregateId String id, String customer) {
  }

  @Test
  @DisplayName("Should read an event back as it was written, without upcasters")
  void anEvent() {
    EventSerde serde = new EventSerde();
    Event event = Event.builder().aggregateType("order").payload(new OrderPlaced("order-1", "Ada")).sequence(1).build();

    Event read = serde.deserializer().deserialize("events.order", serde.serializer().serialize("events.order", event));

    assertThat(read).usingRecursiveComparison().isEqualTo(event);
  }
}
