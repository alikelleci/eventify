package io.github.alikelleci.eventify.core.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.eventify.core.event.Event;

import java.util.List;

/**
 * The outcome of handling a command. It is written to the command's result topic and, when its sender waits for it, to
 * the sender's reply topic, as JSON with a {@code "result"} of {@code "success"} or {@code "failure"}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "result")
@JsonSubTypes({
    @JsonSubTypes.Type(value = CommandResult.Success.class, name = "success"),
    @JsonSubTypes.Type(value = CommandResult.Failure.class, name = "failure")
})
public sealed interface CommandResult permits CommandResult.Success, CommandResult.Failure {

  Command command();

  /**
   * The command was accepted.
   *
   * @param events the events it produced, in the order they were stored; empty when it produced none
   */
  record Success(Command command, List<Event> events) implements CommandResult {
    public Success {
      events = events == null ? List.of() : List.copyOf(events);
    }
  }

  /**
   * The command was rejected: nothing of it was stored.
   *
   * @param cause why, as the root cause's message
   */
  record Failure(Command command, String cause) implements CommandResult {
  }
}
