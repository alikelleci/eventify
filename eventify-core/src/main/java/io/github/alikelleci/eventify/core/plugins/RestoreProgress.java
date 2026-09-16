package io.github.alikelleci.eventify.core.plugins;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * How far the state stores being restored are, per store partition. For a plugin that shows or logs it: return it, or
 * a subclass, from {@link EventifyPlugin#stateRestoreListener()}.
 *
 * <p>A finished partition stays, marked as done, until {@link #clearFinished()}. Taken together, the progress is then
 * over the whole restoration, instead of jumping back each time a partition is done.
 */
public class RestoreProgress implements StateRestoreListener {

  private final Map<TopicPartition, Partition> partitions = new ConcurrentHashMap<>();

  /** One store partition. Restoring can start past offset 0 (e.g. from a checkpoint), so progress is measured from there. */
  public record Partition(String storeName, long startingOffset, long endingOffset, long currentOffset, boolean done) {

    public long total() {
      return Math.max(endingOffset - startingOffset, 0);
    }

    public long restored() {
      return done ? total() : Math.min(Math.max(currentOffset - startingOffset, 0), total());
    }

    /** From 0 to 100. */
    public int percentage() {
      long total = total();
      return total == 0 ? 100 : (int) (restored() * 100 / total);
    }
  }

  /** The partitions of the current restoration, the finished ones included. */
  public Map<TopicPartition, Partition> partitions() {
    return Map.copyOf(partitions);
  }

  /** Whether any partition is still being restored. */
  public boolean restoring() {
    return partitions.values().stream().anyMatch(partition -> !partition.done());
  }

  /** Forgets the finished partitions, e.g. when Kafka Streams changes state and a new restoration can start. */
  public void clearFinished() {
    partitions.values().removeIf(Partition::done);
  }

  @Override
  public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
    partitions.put(topicPartition, new Partition(storeName, startingOffset, endingOffset, startingOffset, false));
  }

  @Override
  public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
    partitions.computeIfPresent(topicPartition, (key, partition) ->
        new Partition(partition.storeName(), partition.startingOffset(), partition.endingOffset(), batchEndOffset, partition.done()));
  }

  @Override
  public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
    partitions.computeIfPresent(topicPartition, (key, partition) ->
        new Partition(partition.storeName(), partition.startingOffset(), partition.endingOffset(), partition.endingOffset(), true));
  }

  /** The partition moved to another instance: it is no longer restored here. */
  @Override
  public void onRestoreSuspended(TopicPartition topicPartition, String storeName, long totalRestored) {
    partitions.remove(topicPartition);
  }
}
