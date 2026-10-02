package io.github.alikelleci.eventify.console.protocol;

/**
 * The request data of each {@link Route}. Optional fields are {@code null} when not given.
 *
 * <p>An aggregate is addressed by its type and its identifier together: one application can hold several aggregates,
 * and two of them can have the same identifier.
 *
 * @see io.github.alikelleci.eventify.console.protocol.Route
 */
public final class Requests {

  private Requests() {
  }

  /** @param cursor the sequence the page starts at, included; {@code null} for the newest events */
  public record Events(String aggregateType, String aggregateId, Long cursor, Integer limit) {
  }

  /** @param sequence the event's sequence in its aggregate */
  public record EventDetail(String aggregateType, String aggregateId, Long sequence) {
  }

  /** @param commandId the command whose events are asked for: the events that name it as their cause */
  public record EventsOfCommand(String aggregateType, String aggregateId, String commandId) {
  }

  /** @param sequence the event to take the state after; {@code null} for the current state */
  public record State(String aggregateType, String aggregateId, Long sequence) {
  }

  public record Commands(String aggregateType, String aggregateId, Integer limit) {
  }
}
