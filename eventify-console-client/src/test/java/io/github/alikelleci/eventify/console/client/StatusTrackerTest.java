package io.github.alikelleci.eventify.console.client;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Status tracker")
class StatusTrackerTest {

  private static final TopicPartition FIRST = new TopicPartition("app-event-store-changelog", 0);
  private static final TopicPartition SECOND = new TopicPartition("app-event-store-changelog", 1);

  private final StatusTracker tracker = new StatusTracker();

  @Test
  @DisplayName("Should report restoring until every partition is restored")
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
  @DisplayName("Should no longer report restoring for a partition that moved to another instance")
  void aPartitionThatMovesToAnotherInstanceIsNoLongerRestoredHere() {
    tracker.onRestoreStart(FIRST, "event-store", 0, 900);
    tracker.onRestoreSuspended(FIRST, "event-store", 300);

    assertThat(tracker.restoring()).isFalse();
  }
}
