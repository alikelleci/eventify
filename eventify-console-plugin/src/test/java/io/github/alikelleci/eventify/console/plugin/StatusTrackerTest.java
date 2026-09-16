package io.github.alikelleci.eventify.console.plugin;

import io.github.alikelleci.eventify.console.protocol.InstanceStatus;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StatusTrackerTest {

  private static final TopicPartition BIG = new TopicPartition("app-event-store-changelog", 0);
  private static final TopicPartition SMALL = new TopicPartition("app-event-store-changelog", 1);

  private final StatusTracker tracker = new StatusTracker();

  @Test
  void aFinishedPartitionStillCountsSoTheProgressDoesNotJumpBack() {
    tracker.onRestoreStart(BIG, "event-store", 0, 900);
    tracker.onRestoreStart(SMALL, "event-store", 0, 100);
    tracker.onBatchRestored(BIG, "event-store", 890, 890);

    assertThat(tracker.restore().percentage()).isEqualTo(89);

    // The big one is done, the small one hasn't started: still 90% of the whole, not 0% of what is left.
    tracker.onRestoreEnd(BIG, "event-store", 900);
    assertThat(tracker.restore()).isEqualTo(new InstanceStatus.Restore(900, 1000, 90));

    tracker.onRestoreEnd(SMALL, "event-store", 100);
    assertThat(tracker.restore()).isNull();
  }

  @Test
  void aNewStateStartsANewRestoration() {
    tracker.onRestoreStart(BIG, "event-store", 0, 900);
    tracker.onRestoreEnd(BIG, "event-store", 900);
    tracker.onChange(KafkaStreams.State.RUNNING, KafkaStreams.State.REBALANCING);

    tracker.onChange(KafkaStreams.State.REBALANCING, KafkaStreams.State.RUNNING);
    tracker.onRestoreStart(SMALL, "event-store", 0, 100);

    assertThat(tracker.restore()).isEqualTo(new InstanceStatus.Restore(0, 100, 0));
  }
}
