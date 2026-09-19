package io.github.alikelleci.eventify.core.serialization;

import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Commands and events are written and read back as they were. Upcasting events: see UpcastingChainTest. */
@DisplayName("Command and event serdes")
class MessageSerdesTest {

  @Topic("commands.order")
  public record PlaceOrder(@AggregateId String id, String customer) {
  }

  @Topic("events.order")
  public record OrderPlaced(@AggregateId String id, String customer) {
  }

  @Test
  @DisplayName("Should read a command back as it was written")
  void aCommand() {
    CommandSerde serde = new CommandSerde();
    Command command = Command.builder().payload(new PlaceOrder("order-1", "Ada")).build();

    Command read = serde.deserializer().deserialize("commands.order", serde.serializer().serialize("commands.order", command));

    assertThat(read).usingRecursiveComparison().isEqualTo(command);
  }

  @Test
  @DisplayName("Should read an event back as it was written, without upcasters")
  void anEvent() {
    EventSerde serde = new EventSerde();
    Event event = Event.builder().payload(new OrderPlaced("order-1", "Ada")).build();

    Event read = serde.deserializer().deserialize("events.order", serde.serializer().serialize("events.order", event));

    assertThat(read).usingRecursiveComparison().isEqualTo(event);
  }
}
