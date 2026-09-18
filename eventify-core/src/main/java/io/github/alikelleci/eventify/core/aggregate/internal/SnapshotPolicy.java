package io.github.alikelleci.eventify.core.aggregate.internal;

import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;

import java.util.Optional;

/**
 * When an aggregate is snapshotted, from its {@link EnableSnapshotting}: every {@code threshold} events, and whether
 * the events before a snapshot are deleted. Without the annotation it is never snapshotted.
 */
public record SnapshotPolicy(int threshold, boolean deleteEvents) {

  public static SnapshotPolicy of(Class<?> aggregateType) {
    return Optional.ofNullable(AnnotationScanner.findAnnotation(aggregateType, EnableSnapshotting.class))
        .map(annotation -> new SnapshotPolicy(Math.max(annotation.threshold(), 0), annotation.deleteEvents()))
        .orElse(new SnapshotPolicy(0, false));
  }

  /**
   * Whether a snapshot is due at {@code version}: when the version passed a multiple of the threshold since the last
   * snapshot. Not only when it is exactly one: a command with several events can step over it.
   */
  public boolean isDue(long lastSnapshotVersion, long version) {
    return threshold > 0 && version / threshold > lastSnapshotVersion / threshold;
  }
}
