package io.github.alikelleci.eventify.core.message;

import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The metadata of a message never changes, also not when a message is made with it. */
@DisplayName("Metadata")
class MetadataTest {

  @Topic("commands.thing")
  record DoThing(@AggregateId String id) {
  }

  @Test
  @DisplayName("Should give a new metadata instead of changing this one")
  void withGivesANewOne() {
    Metadata metadata = Metadata.builder().put("tenant", "acme").build();

    assertThat(metadata.with("tenant", "other")).containsEntry("tenant", "other");
    assertThat(metadata.with("user", "ada")).containsEntry("tenant", "acme").containsEntry("user", "ada");
    assertThat(metadata.withDefault("tenant", "other")).isSameAs(metadata);
    assertThat(metadata).containsOnlyKeys("tenant").containsEntry("tenant", "acme");
  }

  @Test
  @DisplayName("Should refuse to be changed")
  void mapMethodsThatWouldChangeItThrow() {
    Metadata metadata = Metadata.builder().put("tenant", "acme").build();

    assertThatThrownBy(() -> metadata.put("user", "ada")).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> metadata.remove("tenant")).isInstanceOf(UnsupportedOperationException.class);
  }

  /** The same metadata can be given to more than one message: a message that changed it would change the others too. */
  @Test
  @DisplayName("Should not be changed by the message it is given to")
  void aMessageDoesNotChangeTheMetadataItIsGiven() {
    Metadata metadata = Metadata.builder().put("tenant", "acme").build();

    Command command = Command.builder().payload(new DoThing("a")).metadata(metadata).build();

    assertThat(command.getMetadata().getCorrelationId()).isNotBlank();
    assertThat(metadata).doesNotContainKey(MetadataKeys.CORRELATION_ID);
  }

  /** A command of a flow, e.g. a saga: its events are traced with the rest of the flow. */
  @Test
  @DisplayName("Should keep the correlation id the application gave")
  void aGivenCorrelationIdIsKept() {
    Command command = Command.builder().payload(new DoThing("a")).metadata(MetadataKeys.CORRELATION_ID, "saga").build();

    assertThat(command.getMetadata().getCorrelationId()).isEqualTo("saga");
  }
}
