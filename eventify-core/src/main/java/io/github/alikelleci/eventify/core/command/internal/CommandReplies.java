package io.github.alikelleci.eventify.core.command.internal;

import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.exception.CommandExecutionException;
import io.github.alikelleci.eventify.core.command.internal.CommandResult.Failure;
import io.github.alikelleci.eventify.core.message.Metadata;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CAUSE;
import static io.github.alikelleci.eventify.core.message.MetadataKeys.RESULT;

/**
 * How the outcome of a command is sent to its result topic and to the reply topic of its sender: as the command
 * itself, with {@code $result} ("success" or "failure") and, on a failure, {@code $cause} in its metadata. Written by
 * Eventify's topology, read by the command gateway.
 */
public final class CommandReplies {

  private static final String SUCCESS = "success";
  private static final String FAILURE = "failure";

  private CommandReplies() {
  }

  /** The command with its outcome in its metadata. Changes the command's metadata, and returns the command. */
  public static Command toReply(CommandResult result) {
    Command command = result.getCommand();
    if (result instanceof Failure failure) {
      command.getMetadata().put(RESULT, FAILURE);
      command.getMetadata().put(CAUSE, failure.getCause());
    } else {
      command.getMetadata().put(RESULT, SUCCESS);
      command.getMetadata().remove(CAUSE);
    }
    return command;
  }

  /** The failure a reply reports; {@code null} when the command succeeded. */
  public static CommandExecutionException failureOf(Command reply) {
    Metadata metadata = reply.getMetadata();
    return FAILURE.equals(metadata.get(RESULT)) ? new CommandExecutionException(metadata.get(CAUSE)) : null;
  }
}
