package io.github.alikelleci.eventify.core.util;

import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;

import java.util.HashMap;
import java.util.Map;

import static io.github.alikelleci.eventify.core.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.core.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.core.messaging.Metadata.RESULT;
import static org.assertj.core.api.Assertions.assertThat;

public class Matchers {

  public static void assertCommandResult(Command command, Command commandResult, boolean isSuccess) {
    Map<String, String> metadata = new HashMap<>(command.getMetadata());
    metadata.keySet().removeIf(key -> key.startsWith("$") && !key.equals(CORRELATION_ID));

    assertThat(commandResult.getType()).isEqualTo(command.getType());
    assertThat(commandResult.getId()).isEqualTo(command.getId());
    assertThat(commandResult.getAggregateId()).isEqualTo(command.getAggregateId());
    assertThat(commandResult.getTimestamp()).isEqualTo(command.getTimestamp());

    // Metadata
    assertThat(commandResult.getMetadata()).isNotNull();
    assertThat(commandResult.getMetadata()).hasSize(metadata.size() + (isSuccess ? 1 : 2)); // RESULT and/or CAUSE added
    metadata.forEach((key, value) ->
        assertThat(commandResult.getMetadata()).containsEntry(key, value));
    assertThat(commandResult.getMetadata().get(CORRELATION_ID)).isNotNull();
    assertThat(commandResult.getMetadata().get(CORRELATION_ID)).isEqualTo(command.getMetadata().get(CORRELATION_ID));
    assertThat(commandResult.getMetadata().get(RESULT)).isEqualTo(isSuccess ? "success" : "failure");
    if (isSuccess) {
      assertThat(commandResult.getMetadata().get(CAUSE)).isNullOrEmpty();
    } else {
      assertThat(commandResult.getMetadata().get(CAUSE)).isNotNull();
    }

    // Payload
    assertThat(commandResult.getPayload()).isEqualTo(command.getPayload());
  }

  public static void assertEvent(Command command, Event event) {
    Map<String, String> metadata = new HashMap<>(command.getMetadata());
    metadata.keySet().removeIf(key -> key.startsWith("$") && !key.equals(CORRELATION_ID));

    assertThat(event.getType()).isNotNull();
    assertThat(event.getId()).startsWith(command.getAggregateId().concat("@"));
    assertThat(event.getAggregateId()).isEqualTo(command.getAggregateId());
    assertThat(event.getTimestamp()).isEqualTo(command.getTimestamp());

    // Metadata
    assertThat(event.getMetadata()).isNotNull();
    assertThat(event.getMetadata()).hasSize(metadata.size());
    metadata.forEach((key, value) ->
        assertThat(event.getMetadata()).containsEntry(key, value));
    assertThat(event.getMetadata().get(CORRELATION_ID)).isNotNull();
    assertThat(event.getMetadata().get(CORRELATION_ID)).isEqualTo(command.getMetadata().get(CORRELATION_ID));
    assertThat(event.getMetadata().get(RESULT)).isNullOrEmpty();
    assertThat(event.getMetadata().get(CAUSE)).isNullOrEmpty();

    // Payload
    assertThat(event.getPayload()).isNotNull();
    assertThat(event.getPayload())
        .usingRecursiveComparison()
        .isEqualTo(command.getPayload());
  }

  public static void assertEvent(Command command, Event event, Class<?> type) {
    assertEvent(command, event);
    assertThat(event.getType()).isEqualTo(type.getSimpleName());
    assertThat(event.getPayload()).isInstanceOf(type);
  }

  public static void assertSnapshot(Event event, AggregateState state) {
    Map<String, String> metadata = new HashMap<>(event.getMetadata());
    metadata.keySet().removeIf(key -> key.startsWith("$") && !key.equals(CORRELATION_ID));

    assertThat(state.getVersion()).isNotNegative();
    assertThat(state.getEventId()).isEqualTo(event.getId());
    assertThat(state.getType()).isNotNull();
    assertThat(state.getId()).startsWith(event.getAggregateId().concat("@"));
    assertThat(state.getAggregateId()).isEqualTo(event.getAggregateId());
    assertThat(state.getTimestamp()).isEqualTo(event.getTimestamp());

    // Metadata
    assertThat(state.getMetadata()).isNotNull();
    assertThat(state.getMetadata()).hasSize(metadata.size());
    metadata.forEach((key, value) ->
        assertThat(state.getMetadata()).containsEntry(key, value));
    assertThat(state.getMetadata().get(CORRELATION_ID)).isNotNull();
    assertThat(state.getMetadata().get(CORRELATION_ID)).isEqualTo(event.getMetadata().get(CORRELATION_ID));
    assertThat(state.getMetadata().get(RESULT)).isNullOrEmpty();
    assertThat(state.getMetadata().get(CAUSE)).isNullOrEmpty();

    // Payload
    assertThat(state.getPayload()).isNotNull();
  }

  public static void assertSnapshot(Event event, AggregateState state, Class<?> type, long version) {
    assertSnapshot(event, state);
    assertThat(state.getVersion()).isEqualTo(version);
    assertThat(state.getType()).isEqualTo(type.getSimpleName());
    assertThat(state.getPayload()).isInstanceOf(type);
  }
}
