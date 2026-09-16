package io.github.alikelleci.eventify.console.protocol;

/**
 * How one application instance is doing, as it answers {@link Route#STATUS}. Everything in it is read from what the
 * instance already keeps in memory, so asking for it costs the application almost nothing.
 *
 * @param state      the Kafka Streams state: RUNNING, REBALANCING, ERROR, …
 * @param stateForMs how long the instance has been in this state; a duration, so the clocks of the console and the
 *                   instance don't have to agree
 * @param restoring  whether state stores are being restored, after a restart or a rebalance. While this happens the
 *                   instance is REBALANCING and answers no queries about aggregates.
 */
public record InstanceStatus(String state, long stateForMs, boolean restoring) {
}
