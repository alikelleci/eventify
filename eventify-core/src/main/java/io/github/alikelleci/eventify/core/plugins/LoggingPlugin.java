package io.github.alikelleci.eventify.core.plugins;

import io.github.alikelleci.eventify.core.Eventify;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Logs what happens underneath: the state Kafka Streams is in, and the state stores being restored, with how far
 * along they are every ten seconds. Registered by default, and a plugin like any other: to log differently, write
 * your own plugin.
 */
@Slf4j
public class LoggingPlugin implements EventifyPlugin {

  private ScheduledExecutorService scheduler;

  @Override
  public void onStart(Eventify eventify) {
    scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, "eventify-restore-logger");
      thread.setDaemon(true);
      return thread;
    });
    scheduler.scheduleAtFixedRate(this::logProgress, 10, 10, TimeUnit.SECONDS);
  }

  @Override
  public void onStop(Eventify eventify) {
    if (scheduler != null) {
      scheduler.shutdown();
    }
  }

  @Override
  public StateListener stateListener() {
    return (newState, oldState) -> {
      log.info("State changed from {} to {}", oldState, newState);
      progress.clearFinished();
    };
  }

  @Override
  public StateRestoreListener stateRestoreListener() {
    return progress;
  }

  /** Logs each restoration, and every ten seconds how far the ones going on are. */
  private final RestoreProgress progress = new RestoreProgress() {

    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
      log.info("State restoration started: topic={}, partition={}, store={}, startingOffset={}, endingOffset={}",
          topicPartition.topic(), topicPartition.partition(), storeName, startingOffset, endingOffset);
      super.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
      log.info("State restoration ended: topic={}, partition={}, store={}, totalRestored={}",
          topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
      super.onRestoreEnd(topicPartition, storeName, totalRestored);
    }

    @Override
    public void onRestoreSuspended(TopicPartition topicPartition, String storeName, long totalRestored) {
      log.info("State restoration suspended: topic={}, partition={}, store={}, totalRestored={}",
          topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
      super.onRestoreSuspended(topicPartition, storeName, totalRestored);
    }
  };

  private void logProgress() {
    progress.partitions().forEach((topicPartition, partition) -> {
      if (!partition.done()) {
        log.info("State restoration in progress: topic={}, partition={}, store={}, progress={}%",
            topicPartition.topic(), topicPartition.partition(), partition.storeName(), partition.percentage());
      }
    });
  }
}
