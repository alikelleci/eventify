package io.github.alikelleci.eventify.console.plugin;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StatusTrackerTest {

  private static final TopicPartition FIRST = new TopicPartition("app-event-store-changelog", 0);
  private static final TopicPartition SECOND = new TopicPartition("app-event-store-changelog", 1);

  private final StatusTracker tracker = new StatusTracker();

  @Test
  void restoringUntilEveryPartitionIsDone() {
    assertThat(tracker.restoring()).isFalse();

    tracker.onRestoreStart(FIRST, "event-store", 0, 900);
    tracker.onRestoreStart(SECOND, "event-store", 0, 100);
    tracker.onRestoreEnd(FIRST, "event-store", 900);
    assertThat(tracker.restoring()).isTrue();

    tracker.onRestoreEnd(SECOND, "event-store", 100);
    assertThat(tracker.restoring()).isFalse();
  }

  @Test
  void aPartitionThatMovesToAnotherInstanceIsNoLongerRestoredHere() {
    tracker.onRestoreStart(FIRST, "event-store", 0, 900);
    tracker.onRestoreSuspended(FIRST, "event-store", 300);

    assertThat(tracker.restoring()).isFalse();
  }
}
