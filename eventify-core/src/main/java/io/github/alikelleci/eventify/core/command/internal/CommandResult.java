package io.github.alikelleci.eventify.core.command.internal;

import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.event.Event;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;

/** The outcome of handling a command. How it is sent to the result and reply topics: see {@link CommandReplies}. */
public interface CommandResult {

  Command getCommand();

  @Value
  @Builder
  class Success implements CommandResult {
    Command command;
    @Singular
    List<Event> events;
  }

  @Value
  @Builder
  class Failure implements CommandResult {
    Command command;
    String cause;
  }

}
