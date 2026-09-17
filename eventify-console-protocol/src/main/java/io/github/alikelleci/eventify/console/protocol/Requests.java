package io.github.alikelleci.eventify.console.protocol;

/** The request data of each {@link Route}. Optional fields are {@code null} when not given. */
public final class Requests {

  private Requests() {
  }

  public record Events(String aggregateId, String cursor, Integer limit) {
  }

  public record EventDetail(String aggregateId, String eventId) {
  }

  /**
   * @param commandId     the command whose events are asked for
   * @param correlationId the command's correlation id, optional: finds the events of the command stored before events
   *                      named the command that produced them
   */
  public record EventsOfCommand(String aggregateId, String commandId, String correlationId) {
  }

  public record State(String aggregateId, String eventId) {
  }

  public record Commands(String aggregateId, Integer limit) {
  }
}
