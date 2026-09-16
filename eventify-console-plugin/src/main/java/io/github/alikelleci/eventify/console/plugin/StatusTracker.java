package io.github.alikelleci.eventify.console.plugin;

import io.github.alikelleci.eventify.console.protocol.InstanceStatus;
import io.github.alikelleci.eventify.core.plugins.RestoreProgress;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;

/**
 * Follows what Kafka Streams is doing, so the console can be told at any moment: when the state last changed, and how
 * far the state stores being restored are. Kept here rather than asked for on the spot, because both are only known
 * while they happen, and because asking Kafka for it would cost a round trip.
 */
public class StatusTracker extends RestoreProgress implements StateListener {

  /** When the state last changed, on this instance's clock. Starts at the moment the application starts. */
  private volatile long stateSince = System.currentTimeMillis();

  /** How long the instance has been in its current state. */
  public long stateForMs() {
    return System.currentTimeMillis() - stateSince;
  }

  /** What is being restored right now, over all store partitions, or {@code null} when nothing is. */
  public InstanceStatus.Restore restore() {
    if (!restoring()) {
      return null;
    }
    long restored = 0;
    long total = 0;
    for (Partition partition : partitions().values()) {
      restored += partition.restored();
      total += partition.total();
    }
    return total > 0 ? new InstanceStatus.Restore(restored, total, (int) (restored * 100 / total)) : null;
  }

  @Override
  public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
    stateSince = System.currentTimeMillis();
    // A new state starts a new picture: what was restored before no longer counts.
    clearFinished();
  }
}
