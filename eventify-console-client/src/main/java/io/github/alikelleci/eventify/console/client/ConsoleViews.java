package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.util.RawValue;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.event.Event;

import java.util.List;

/** What the console is answered: pages of events and commands, and the outcome of each query. */
final class ConsoleViews {

  private ConsoleViews() {
  }

  /**
   * The aggregate's newest commands.
   *
   * @param lookbackDays how far back the commands were read: older commands are not in the page
   * @param truncated    whether more commands were found than the page holds: the oldest ones are left out
   */
  record CommandsPage(List<CommandView> commands, long lookbackDays, boolean truncated) {}

  /**
   * A command as the console shows it: the command's own fields, with how its handling ended.
   *
   * @param result "success" or "failure", as in the command's {@link CommandResult}
   * @param cause  why it failed; {@code null} when it succeeded
   */
  record CommandView(@JsonUnwrapped Command command, String result, String cause) {

    static CommandView of(CommandResult result) {
      return result instanceof CommandResult.Failure failure
          ? new CommandView(failure.command(), "failure", failure.cause())
          : new CommandView(result.command(), "success", null);
    }
  }
  record EventsPage(List<Event> events, Long nextCursor) {}
  /**
   * An event with the state after and before it. A state is {@code null} when there is none, or when it is unknown:
   * {@code stateKnown} and {@code previousStateKnown} tell which. A state is unknown when the events before it were
   * deleted at a snapshot. The states are {@link AggregateState}s as JSON, see {@link AggregateHistory}.
   */
  record EventDetail(Event event, RawValue state, RawValue previousState,
                     boolean stateKnown, boolean previousStateKnown) {

    /** Both states are known; either can still be {@code null}, which then means there is no state. */
    static EventDetail known(Event event, RawValue state, RawValue previousState) {
      return new EventDetail(event, state, previousState, true, true);
    }

    /** The state after the event is known, the one before it isn't: the events before it were deleted at a snapshot. */
    static EventDetail withUnknownPreviousState(Event event, RawValue state) {
      return new EventDetail(event, state, null, true, false);
    }

    /** Neither state can be rebuilt: the events before this one were deleted at a snapshot. */
    static EventDetail withUnknownStates(Event event) {
      return new EventDetail(event, null, null, false, false);
    }
  }
  record CommandEventsPage(List<Event> events) {}

  /** The outcome of a query, as the console is told it, with the answer when it's {@link ReplyHeader.Status#OK}. */
  record Result<T>(ReplyHeader header, T value) {
    static <T> Result<T> ok(T value) {
      return new Result<>(ReplyHeader.ok(), value);
    }

    static <T> Result<T> notFound() {
      return new Result<>(ReplyHeader.notFound(), null);
    }

    /** Another instance owns the aggregate; {@code owner} is its node id. */
    static <T> Result<T> notOwner(String owner) {
      return new Result<>(ReplyHeader.notOwner(owner), null);
    }

    static <T> Result<T> unavailable(String reason) {
      return new Result<>(ReplyHeader.unavailable(reason), null);
    }

    static <T> Result<T> badRequest(String reason) {
      return new Result<>(ReplyHeader.badRequest(reason), null);
    }

    static <T> Result<T> unreadable(String reason) {
      return new Result<>(ReplyHeader.unreadable(reason), null);
    }

    boolean isOk() {
      return header.status() == ReplyHeader.Status.OK;
    }
  }
}
