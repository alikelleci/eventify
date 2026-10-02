package io.github.alikelleci.eventify.core.plugin;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Logs Kafka Streams state changes and store restoration progress; registered by default. */
@Slf4j
public class LoggingPlugin implements EventifyPlugin {

  private final Map<TopicPartition, Stats> restoring = new ConcurrentHashMap<>();
  private ScheduledExecutorService scheduler;

  @Override
  public void onStart(PluginContext context) {
    scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, "eventify-restore-logger");
      thread.setDaemon(true);
      return thread;
    });
    scheduler.scheduleAtFixedRate(this::logProgress, 10, 10, TimeUnit.SECONDS);
  }

  @Override
  public void onStop(PluginContext context) {
    if (scheduler != null) {
      scheduler.shutdown();
    }
    restoring.clear();
  }

  @Override
  public StateListener stateListener() {
    return (newState, oldState) -> log.info("State changed from {} to {}", oldState, newState);
  }

  @Override
  public StateRestoreListener stateRestoreListener() {
    return restoreLogger;
  }

  /** Logs each restoration, and every ten seconds how far the ones going on are. */
  private final StateRestoreListener restoreLogger = new StateRestoreListener() {

    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
      log.info("State restoration started: topic={}, partition={}, store={}, startingOffset={}, endingOffset={}",
          topicPartition.topic(), topicPartition.partition(), storeName, startingOffset, endingOffset);
      restoring.put(topicPartition, Stats.builder()
          .storeName(storeName)
          .startingOffset(startingOffset)
          .currentOffset(startingOffset)
          .endingOffset(endingOffset)
          .build());
    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
      Stats stats = restoring.get(topicPartition);
      if (stats != null) {
        stats.setCurrentOffset(batchEndOffset);
      }
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
      log.info("State restoration ended: topic={}, partition={}, store={}, totalRestored={}",
          topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
      restoring.remove(topicPartition);
    }

    @Override
    public void onRestoreSuspended(TopicPartition topicPartition, String storeName, long totalRestored) {
      log.info("State restoration suspended: topic={}, partition={}, store={}, totalRestored={}",
          topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
      restoring.remove(topicPartition);
    }
  };

  private void logProgress() {
    restoring.forEach((topicPartition, stats) -> {
      if (stats.getCurrentOffset() < stats.getEndingOffset()) {
        log.info("State restoration in progress: topic={}, partition={}, store={}, progress={}%",
            topicPartition.topic(), topicPartition.partition(), stats.getStoreName(), stats.progressPercentage());
      }
    });
  }

  @Data
  @Builder(toBuilder = true)
  static class Stats {
    private String storeName;
    private long startingOffset;
    private long endingOffset;
    private long currentOffset;

    /** Percentage done, measured from the starting offset: restoration can start from a checkpoint. */
    int progressPercentage() {
      long total = endingOffset - startingOffset;
      if (total <= 0) {
        return 100;
      }
      long restored = Math.min(Math.max(currentOffset - startingOffset, 0), total);
      return (int) (restored * 100 / total);
    }
  }
}
