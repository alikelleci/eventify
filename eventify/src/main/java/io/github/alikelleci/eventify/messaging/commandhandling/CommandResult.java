package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;


public interface CommandResult {

  Command getCommand();

  @Value
  @Builder
  class Success implements CommandResult {
    private Command command;
    @Singular
    private List<Event> events;

    @Override
    public Command getCommand() {
      return command.toBuilder()
          .metadata(command.getMetadata().toBuilder()
              .entry(Metadata.RESULT, "success")
              .build())
          .build();
    }
  }

  @Value
  @Builder
  class Failure implements CommandResult {
    private Command command;
    private String cause;

    @Override
    public Command getCommand() {
      return command.toBuilder()
          .metadata(command.getMetadata().toBuilder()
              .entry(Metadata.RESULT, "failure")
              .entry(Metadata.CAUSE, cause)
              .build())
          .build();
    }
  }

}
