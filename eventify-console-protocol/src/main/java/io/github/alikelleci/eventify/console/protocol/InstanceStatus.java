package io.github.alikelleci.eventify.console.protocol;

/**
 * How one application instance is doing, as it answers {@link Route#STATUS}. Everything in it is read from what the
 * instance already keeps in memory, so asking for it costs the application almost nothing.
 *
 * @param state           the Kafka Streams state: RUNNING, REBALANCING, ERROR, …
 * @param stateForMs      how long the instance has been in this state; a duration, so the clocks of the console and
 *                        the instance don't have to agree
 * @param commandsInQueue commands waiting on the instance's command topics, or {@code null} when it can't be measured
 *                        yet (e.g. before the first fetch)
 * @param restore         the state stores being restored, or {@code null} when nothing is being restored
 */
public record InstanceStatus(String state, long stateForMs, Long commandsInQueue, Restore restore) {

  /**
   * Restoring state stores after a restart or a rebalance. While this happens the instance is REBALANCING and answers
   * no queries about aggregates.
   */
  public record Restore(long restored, long total, int percentage) {
  }
}
