package io.github.alikelleci.eventify.core.message;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Messages are stored and sent as JSON: what is derived from a message is not written with it. */
@DisplayName("Message JSON")
class MessageJsonTest {

  @Topic("things")
  record Thing(@AggregateId String id) {
  }

  @Test
  @DisplayName("Should write a command and an event without their topic, correlation id or causation id as fields")
  void derivedValuesAreNotWritten() {
    Command command = Command.builder().payload(new Thing("a")).build();
    Event event = Event.builder().payload(new Thing("a")).metadata(MetadataKeys.CAUSATION_ID, command.getId()).build();

    for (Object message : new Object[]{command, event}) {
      JsonNode json = EventifyObjectMapper.create().valueToTree(message);
      assertThat(json.has("topic")).as("topic").isFalse();
      assertThat(json.path("metadata").has("correlationId")).as("correlationId").isFalse();
      assertThat(json.path("metadata").has("causationId")).as("causationId").isFalse();
      assertThat(json.path("metadata").has(MetadataKeys.CORRELATION_ID)).as("the metadata itself").isTrue();
    }
  }
}
