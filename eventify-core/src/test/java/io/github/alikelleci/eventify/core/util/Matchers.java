package io.github.alikelleci.eventify.core.util;

import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;

import static io.github.alikelleci.eventify.core.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.core.messaging.Metadata.RESULT;
import static org.assertj.core.api.Assertions.assertThat;

public class Matchers {

  public static void assertCommandResult(Command command, Command commandResult, boolean isSuccess) {
    assertThat(commandResult)
        .usingRecursiveComparison(RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("metadata.$result", "metadata.$cause")
            .build())
        .isEqualTo(command);

    if (isSuccess) {
      assertThat(commandResult.getMetadata().get(RESULT)).isEqualTo("success");
      assertThat(commandResult.getMetadata().get(CAUSE)).isBlank();
    } else {
      assertThat(commandResult.getMetadata().get(RESULT)).isEqualTo("failure");
      assertThat(commandResult.getMetadata().get(CAUSE)).isNotBlank();
    }
  }

  public static void assertEvent(Command command, Event event) {
    assertThat(event)
        .usingRecursiveComparison(RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("id", "type", "revision")
            .build())
        .isEqualTo(command);
  }

  public static void assertEvent(Command command, Event event, Class<?> type) {
    assertEvent(command, event);
    assertThat(event.getType()).isEqualTo(type.getSimpleName());
    assertThat(event.getPayload()).isInstanceOf(type);
  }

  public static void assertSnapshot(Event event, AggregateState state) {
    assertThat(state)
        .usingRecursiveComparison(RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("id", "type", "payload", "eventId", "version")
            .build())
        .isEqualTo(event);

    assertThat(state.getEventId()).isEqualTo(event.getId());
    assertThat(state.getVersion()).isNotNegative();

  }

  public static void assertSnapshot(Event event, AggregateState state, Class<?> type, long version) {
    assertSnapshot(event, state);
    assertThat(state.getVersion()).isEqualTo(version);
    assertThat(state.getType()).isEqualTo(type.getSimpleName());
    assertThat(state.getPayload()).isInstanceOf(type);
  }
}
