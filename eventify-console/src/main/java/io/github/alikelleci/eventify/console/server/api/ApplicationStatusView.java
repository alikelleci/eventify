package io.github.alikelleci.eventify.console.server.api;

/**
 * How an application is doing, as the UI shows it: the instances are asked and their answers are combined.
 *
 * @param name             the application id
 * @param state            the state of the instance that is worst off: an error weighs heavier than a rebalance
 * @param stateForMs       how long the application has been in that state, from the instance that has been in it longest
 * @param inState          how many instances are in that state; fewer than {@code answered} means only some are
 * @param commandsInQueue  the commands waiting over all instances, or {@code null} when none could measure it
 * @param restore          what is being restored over all instances, or {@code null} when nothing is
 * @param instances        the instances connected right now
 * @param answered         how many of them answered; fewer means the numbers are only part of the picture
 */
public record ApplicationStatusView(String name, String state, long stateForMs, int inState, Long commandsInQueue,
                                    Restore restore, int instances, int answered) {

  public record Restore(long restored, long total, int percentage) {
  }
}
