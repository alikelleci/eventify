package io.github.alikelleci.eventify.core.messaging.commandhandling;

import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;

import static io.github.alikelleci.eventify.core.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.core.messaging.Metadata.RESULT;


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
