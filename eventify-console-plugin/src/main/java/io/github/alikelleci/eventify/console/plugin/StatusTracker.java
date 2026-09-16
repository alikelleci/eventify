package io.github.alikelleci.eventify.console.plugin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Follows what Kafka Streams is doing, so the console can be told at any moment: when the state last changed, and
 * whether state stores are being restored. Kept here rather than asked for on the spot, because both are only known
 * while they happen.
 */
public class StatusTracker implements StateListener, StateRestoreListener {

  /** When the state last changed, on this instance's clock. Starts at the moment the application starts. */
  private volatile long stateSince = System.currentTimeMillis();

  /** The store partitions being restored right now. */
  private final Set<TopicPartition> restoring = ConcurrentHashMap.newKeySet();

  /** How long the instance has been in its current state. */
  public long stateForMs() {
    return System.currentTimeMillis() - stateSince;
  }

  /** Whether any state store is being restored right now. */
  public boolean restoring() {
    return !restoring.isEmpty();
  }

  @Override
  public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
    stateSince = System.currentTimeMillis();
  }

  @Override
  public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
    restoring.add(topicPartition);
  }

  @Override
  public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
  }

  @Override
  public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
    restoring.remove(topicPartition);
  }

  /** The partition moved to another instance: it is no longer restored here. */
  @Override
  public void onRestoreSuspended(TopicPartition topicPartition, String storeName, long totalRestored) {
    restoring.remove(topicPartition);
  }
}
