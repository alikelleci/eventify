package io.github.alikelleci.eventify.console.protocol;

/** The request data of each {@link Route}. Optional fields are {@code null} when not given. */
public final class Requests {

  private Requests() {
  }

  /** @param cursor the sequence the page starts at, included; {@code null} for the newest events */
  public record Events(String aggregateId, Long cursor, Integer limit) {
  }

  /** @param sequence the event's sequence in its aggregate */
  public record EventDetail(String aggregateId, Long sequence) {
  }

  /**
   * @param commandId     the command whose events are asked for
   * @param correlationId the command's correlation id, optional: finds the events of the command stored before events
   *                      named the command that produced them
   */
  public record EventsOfCommand(String aggregateId, String commandId, String correlationId) {
  }

  /** @param sequence the event to take the state after; {@code null} for the current state */
  public record State(String aggregateId, Long sequence) {
  }

  public record Commands(String aggregateId, Integer limit) {
  }
}
