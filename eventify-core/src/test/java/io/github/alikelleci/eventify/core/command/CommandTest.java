package io.github.alikelleci.eventify.core.command;

import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.MetadataKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** How an application makes a command: a payload, and as much metadata as it wants to carry along. */
@DisplayName("Command")
class CommandTest {

  @Topic("commands.order")
  public record PlaceOrder(@AggregateId String id, String customer) {
  }

  @Test
  @DisplayName("Should keep every metadata entry it was given, one call at a time")
  void metadataEntriesAddUp() {
    Command command = Command.builder()
        .payload(new PlaceOrder("order-1", "Ada"))
        .metadata("tenant", "acme")
        .metadata("user", "ada")
        .build();

    assertThat(command.getMetadata())
        .containsEntry("tenant", "acme")
        .containsEntry("user", "ada")
        .containsKey(MetadataKeys.CORRELATION_ID);
  }

  /** A map, e.g. the metadata of the message this command comes from, together with entries of its own. */
  @Test
  @DisplayName("Should take metadata as a map too, next to single entries")
  void metadataCanComeFromAMap() {
    Metadata incoming = Metadata.of("tenant", "acme");

    Command command = Command.builder()
        .payload(new PlaceOrder("order-1", "Ada"))
        .metadata(incoming)
        .metadata(Map.of("source", "api"))
        .metadata("user", "ada")
        .build();

    assertThat(command.getMetadata())
        .containsEntry("tenant", "acme")
        .containsEntry("source", "api")
        .containsEntry("user", "ada");
  }

  @Test
  @DisplayName("Should know what it is, without being told")
  void whatACommandKnowsAboutItself() {
    Instant before = Instant.now();

    Command command = Command.builder().payload(new PlaceOrder("order-1", "Ada")).build();

    assertThat(command.getId()).isNotBlank();
    assertThat(command.getType()).isEqualTo("PlaceOrder");
    assertThat(command.getAggregateId()).isEqualTo("order-1");
    assertThat(command.getTopic().value()).isEqualTo("commands.order");
    assertThat(command.getTimestamp()).isBetween(before, Instant.now());
  }
}
