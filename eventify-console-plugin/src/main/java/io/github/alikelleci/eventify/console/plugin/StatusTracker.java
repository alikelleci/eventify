package io.github.alikelleci.eventify.console.plugin;

import io.github.alikelleci.eventify.console.protocol.InstanceStatus;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Follows what Kafka Streams is doing, so the console can be told at any moment: when the state last changed, and how
 * far the state stores being restored are. Kept here rather than asked for on the spot, because both are only known
 * while they happen, and because asking Kafka for it would cost a round trip.
 */
public class StatusTracker implements StateListener, StateRestoreListener {

  /** When the state last changed, on this instance's clock. Starts at the moment the application starts. */
  private volatile long stateSince = System.currentTimeMillis();

  private final Map<TopicPartition, Progress> restoring = new ConcurrentHashMap<>();

  /** How long the instance has been in its current state. */
  public long stateForMs() {
    return System.currentTimeMillis() - stateSince;
  }

  /** What is being restored right now, over all store partitions, or {@code null} when nothing is. */
  public InstanceStatus.Restore restore() {
    long restored = 0;
    long total = 0;
    for (Progress progress : restoring.values()) {
      restored += progress.restored();
      total += progress.total();
    }
    if (total <= 0) {
      return null;
    }
    return new InstanceStatus.Restore(restored, total, (int) (restored * 100 / total));
  }

  @Override
  public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
    stateSince = System.currentTimeMillis();
  }

  @Override
  public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
    restoring.put(topicPartition, new Progress(startingOffset, endingOffset, startingOffset));
  }

  @Override
  public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
    Progress progress = restoring.get(topicPartition);
    if (progress != null) {
      progress.currentOffset = batchEndOffset;
    }
  }

  @Override
  public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
    restoring.remove(topicPartition);
  }

  @Override
  public void onRestoreSuspended(TopicPartition topicPartition, String storeName, long totalRestored) {
    restoring.remove(topicPartition);
  }

  /** One store partition being restored. Restoring can start past offset 0, so progress is measured from there. */
  private static final class Progress {
    private final long startingOffset;
    private final long endingOffset;
    private volatile long currentOffset;

    Progress(long startingOffset, long endingOffset, long currentOffset) {
      this.startingOffset = startingOffset;
      this.endingOffset = endingOffset;
      this.currentOffset = currentOffset;
    }

    long total() {
      return Math.max(endingOffset - startingOffset, 0);
    }

    long restored() {
      return Math.min(Math.max(currentOffset - startingOffset, 0), total());
    }
  }
}
