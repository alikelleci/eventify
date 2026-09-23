package io.github.alikelleci.eventify.core.aggregate.internal;

import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.internal.reflection.PerClass;

import java.util.Optional;
import java.util.function.Function;

/** From {@link EnableSnapshotting}: snapshot every {@code threshold} events, optionally deleting the events before. */
public record SnapshotPolicy(int threshold, boolean deleteEvents) {

  private static final Function<Class<?>, SnapshotPolicy> POLICY = PerClass.of(aggregateType ->
      Optional.ofNullable(AnnotationScanner.findAnnotation(aggregateType, EnableSnapshotting.class))
          .map(annotation -> new SnapshotPolicy(Math.max(annotation.threshold(), 0), annotation.deleteEvents()))
          .orElse(new SnapshotPolicy(0, false)));

  public static SnapshotPolicy of(Class<?> aggregateType) {
    return POLICY.apply(aggregateType);
  }

  /** Whether a threshold multiple was passed since the last snapshot (0 when none); a command can step over one. */
  public boolean isSnapshotDue(long lastSnapshotVersion, long aggregateVersion) {
    return threshold > 0 && aggregateVersion / threshold > lastSnapshotVersion / threshold;
  }
}
