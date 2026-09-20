package io.github.alikelleci.eventify.core.aggregate.internal;

import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.internal.reflection.PerClass;

import java.util.Optional;
import java.util.function.Function;

/**
 * When an aggregate is snapshotted, from its {@link EnableSnapshotting}: every {@code threshold} events, and whether
 * the events before a snapshot are deleted. Without the annotation it is never snapshotted.
 */
public record SnapshotPolicy(int threshold, boolean deleteEvents) {

  private static final Function<Class<?>, SnapshotPolicy> POLICY = PerClass.of(aggregateType ->
      Optional.ofNullable(AnnotationScanner.findAnnotation(aggregateType, EnableSnapshotting.class))
          .map(annotation -> new SnapshotPolicy(Math.max(annotation.threshold(), 0), annotation.deleteEvents()))
          .orElse(new SnapshotPolicy(0, false)));

  public static SnapshotPolicy of(Class<?> aggregateType) {
    return POLICY.apply(aggregateType);
  }

  /**
   * Whether the aggregate is to be snapshotted now: when it passed a multiple of the threshold since its last snapshot.
   * Not only when it is exactly one: a command with several events can step over it.
   *
   * @param lastSnapshotVersion the version of its last snapshot; 0 when it has none
   * @param aggregateVersion    the version the aggregate is at now: the sequence of its last event
   */
  public boolean isSnapshotDue(long lastSnapshotVersion, long aggregateVersion) {
    return threshold > 0 && aggregateVersion / threshold > lastSnapshotVersion / threshold;
  }
}
