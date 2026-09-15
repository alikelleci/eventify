package io.github.alikelleci.eventify.console.protocol;

/** The request data of each {@link Route}. Optional fields are {@code null} when not given. */
public final class Requests {

  private Requests() {
  }

  public record Events(String aggregateId, String cursor, Integer limit) {
  }

  public record EventDetail(String aggregateId, String eventId) {
  }

  public record EventsByCorrelation(String aggregateId, String correlationId) {
  }

  public record State(String aggregateId, String eventId) {
  }

  public record Commands(String aggregateId, Integer limit) {
  }
}
