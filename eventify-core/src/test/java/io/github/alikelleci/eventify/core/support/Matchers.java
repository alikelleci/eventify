package io.github.alikelleci.eventify.core.support;

import io.github.alikelleci.eventify.core.command.CommandResult;

/** Short descriptions of a command's result, to compare several results in one assertion. */
public class Matchers {

  private Matchers() {
  }

  /** "success" or "failure", as the result is written. */
  public static String outcome(CommandResult result) {
    return result instanceof CommandResult.Success ? "success" : "failure";
  }

  /** Why the command failed; {@code null} when it succeeded. */
  public static String causeOf(CommandResult result) {
    return result instanceof CommandResult.Failure failure ? failure.cause() : null;
  }
}
