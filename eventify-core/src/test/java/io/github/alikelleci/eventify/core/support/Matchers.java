package io.github.alikelleci.eventify.core.support;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.event.Event;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSATION_ID;
import static org.assertj.core.api.Assertions.assertThat;

public class Matchers {

  public static void assertCommandResult(Command command, CommandResult commandResult, boolean isSuccess) {
    assertThat(commandResult.command())
        .usingRecursiveComparison()
        .isEqualTo(command);

    if (isSuccess) {
      assertThat(commandResult).isInstanceOf(CommandResult.Success.class);
    } else {
      assertThat(commandResult).isInstanceOf(CommandResult.Failure.class);
      assertThat(causeOf(commandResult)).isNotBlank();
    }
  }

  /** "success" or "failure", as the result is written. */
  public static String outcome(CommandResult result) {
    return result instanceof CommandResult.Success ? "success" : "failure";
  }

  /** Why the command failed; {@code null} when it succeeded. */
  public static String causeOf(CommandResult result) {
    return result instanceof CommandResult.Failure failure ? failure.cause() : null;
  }


  public static void assertEvent(Command command, Event event, Class<?> type) {
    assertThat(event)
        .usingRecursiveComparison(RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("id", "type", "revision", "metadata.$causationId")
            .build())
        .isEqualTo(command);

    assertThat(event.getId()).isNotBlank();
    assertThat(event.getMetadata()).containsEntry(CAUSATION_ID, command.getId());
    assertThat(event.getType()).isEqualTo(type.getSimpleName());
    assertThat(event.getRevision()).isNotNegative();
    assertThat(event.getPayload()).isInstanceOf(type);
  }


  public static void assertSnapshot(Event event, AggregateState state, Class<?> type, long version) {
    assertThat(state)
        .usingRecursiveComparison(RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("id", "type", "payload", "eventId", "version")
            .build())
        .isEqualTo(event);

    assertThat(state.getId()).isNotBlank();
    assertThat(state.getType()).isEqualTo(type.getSimpleName());
    assertThat(state.getEventId()).isEqualTo(event.getId());
    assertThat(state.getVersion()).isEqualTo(version);
    assertThat(state.getPayload()).isInstanceOf(type);
  }
}
