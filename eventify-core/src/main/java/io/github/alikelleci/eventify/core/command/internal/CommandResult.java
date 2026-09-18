package io.github.alikelleci.eventify.core.command.internal;

import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.event.Event;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;

import static io.github.alikelleci.eventify.core.message.Metadata.CAUSE;
import static io.github.alikelleci.eventify.core.message.Metadata.RESULT;


public interface CommandResult {

  Command getCommand();

  @Value
  @Builder
  class Success implements CommandResult {
    Command command;
    @Singular
    List<Event> events;

    @Override
    public Command getCommand() {
      command.getMetadata().put(RESULT, "success");
      command.getMetadata().remove(CAUSE);

      return command;
    }
  }

  @Value
  @Builder
  class Failure implements CommandResult {
    Command command;
    String cause;

    @Override
    public Command getCommand() {
      command.getMetadata().put(RESULT, "failure");
      command.getMetadata().put(CAUSE, cause);

      return command;
    }
  }

}
