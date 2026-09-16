package io.github.alikelleci.eventify.console.server.api;

/**
 * How an application is doing, as the UI shows it: the instances are asked and their answers are combined.
 *
 * @param state      the state of the instance that is worst off: an error weighs heavier than a rebalance; {@code null}
 *                   when no instance answered
 * @param stateForMs how long the application has been in that state, from the instance that has been in it longest
 * @param inState    how many instances are in that state; fewer than {@code answered} means only some are
 * @param restore    what is being restored, or {@code null} when nothing is
 * @param answered   how many instances answered; fewer than are connected means the numbers are only part of the picture
 */
public record ApplicationStatusView(String state, long stateForMs, int inState, Restore restore, int answered) {

  /**
   * @param percentage how far the slowest instance is. It never goes down while the same work is being restored: each
   *                   instance only goes up, and when the slowest is done the next one is at least as far
   * @param instances  how many instances are restoring
   */
  public record Restore(int percentage, int instances) {
  }
}
