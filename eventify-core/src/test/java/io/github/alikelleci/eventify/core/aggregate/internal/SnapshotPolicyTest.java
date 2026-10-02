package io.github.alikelleci.eventify.core.aggregate.internal;

import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Snapshot policy")
class SnapshotPolicyTest {

  @EnableSnapshotting(threshold = 10, deleteEvents = true)
  static class Snapshotted {
  }

  static class NotSnapshotted {
  }

  @Test
  @DisplayName("Should read the threshold and deleteEvents from @EnableSnapshotting")
  void readsTheAnnotation() {
    assertThat(SnapshotPolicy.of(Snapshotted.class)).isEqualTo(new SnapshotPolicy(10, true));
    assertThat(SnapshotPolicy.of(NotSnapshotted.class)).isEqualTo(new SnapshotPolicy(0, false));
  }

  @Test
  @DisplayName("Should be due when the version passed a multiple of the threshold, also when a command steps over it")
  void aSnapshotIsDueWhenAMultipleIsPassed() {
    SnapshotPolicy policy = new SnapshotPolicy(10, false);

    assertThat(policy.isSnapshotDue(0, 9)).isFalse();
    assertThat(policy.isSnapshotDue(0, 10)).isTrue();
    assertThat(policy.isSnapshotDue(8, 12)).isTrue();   // several events: stepped over 10
    assertThat(policy.isSnapshotDue(10, 19)).isFalse(); // snapshotted at 10 already
    assertThat(policy.isSnapshotDue(10, 20)).isTrue();
  }

  @Test
  @DisplayName("Should never be due without a threshold")
  void neverDueWithoutAThreshold() {
    assertThat(new SnapshotPolicy(0, false).isSnapshotDue(0, 1_000)).isFalse();
  }
}
