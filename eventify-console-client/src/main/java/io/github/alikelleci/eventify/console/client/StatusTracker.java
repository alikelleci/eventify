package io.github.alikelleci.eventify.console.client;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Follows when the Kafka Streams state last changed and whether stores are being restored. Kafka Streams only reports
 * both as they happen, so they are kept here.
 */
class StatusTracker implements StateListener, StateRestoreListener {

  /** When the state last changed, on this instance's clock. Starts at the moment the application starts. */
  private volatile long stateSince = System.currentTimeMillis();

  /** The store partitions being restored right now. */
  private final Set<TopicPartition> restoring = ConcurrentHashMap.newKeySet();

  /** How long the instance has been in its current state. */
  long stateForMs() {
    return System.currentTimeMillis() - stateSince;
  }

  /** Whether any state store is being restored right now. */
  boolean restoring() {
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

  /** Required by Kafka Streams, but not needed here: only whether something is being restored matters, not how far. */
  @Override
  public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
  }

  @Override
  public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
    restoring.remove(topicPartition);
  }

  /** The partition moved to another instance: it is no longer being restored here. */
  @Override
  public void onRestoreSuspended(TopicPartition topicPartition, String storeName, long totalRestored) {
    restoring.remove(topicPartition);
  }
}
