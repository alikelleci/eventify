package io.github.alikelleci.eventify.core.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.eventify.core.event.Event;

import java.util.List;

/** The outcome of a command, written to its result topic and, when awaited, to the sender's reply topic. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "result")
@JsonSubTypes({
    @JsonSubTypes.Type(value = CommandResult.Success.class, name = "success"),
    @JsonSubTypes.Type(value = CommandResult.Failure.class, name = "failure")
})
public sealed interface CommandResult permits CommandResult.Success, CommandResult.Failure {

  Command command();

  /** The command was accepted; {@code events} in stored order, empty when none. */
  record Success(Command command, List<Event> events) implements CommandResult {
    public Success {
      events = events == null ? List.of() : List.copyOf(events);
    }
  }

  /** The command was rejected and nothing of it was stored; {@code cause} is the root cause's message. */
  record Failure(Command command, String cause) implements CommandResult {
  }
}
