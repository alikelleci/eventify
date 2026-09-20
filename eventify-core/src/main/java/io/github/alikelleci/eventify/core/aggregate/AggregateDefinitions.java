package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.aggregate.internal.AggregateTypes;
import io.github.alikelleci.eventify.core.aggregate.internal.SnapshotPolicy;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The immutable aggregate configuration of one Eventify instance. It identifies the aggregate types it handles and
 * their snapshot policies; it does not access aggregate state or stores.
 */
public final class AggregateDefinitions {

  private final Map<String, Class<?>> aggregateClasses;

  public AggregateDefinitions(Collection<Class<?>> aggregateClasses) {
    this.aggregateClasses = aggregateClasses.stream()
        .collect(Collectors.toUnmodifiableMap(AggregateTypes::of, aggregateClass -> aggregateClass));
  }

  /** Refuses an aggregate type this Eventify instance does not handle. */
  public void requireType(String aggregateType) {
    if (!aggregateClasses.containsKey(aggregateType)) {
      throw new IllegalArgumentException("This Eventify instance has no aggregate named '" + aggregateType + "'. It handles " + aggregateClasses.keySet() + ".");
    }
  }

  /** Whether a state has advanced far enough since its last snapshot to write another one. */
  public boolean isSnapshotDue(String aggregateType, long snapshotVersion, long aggregateVersion) {
    return snapshotPolicyOf(aggregateType).isSnapshotDue(snapshotVersion, aggregateVersion);
  }

  /** Whether events before a stored snapshot of this aggregate type can be removed. */
  public boolean deletesEventsAtSnapshot(String aggregateType) {
    return snapshotPolicyOf(aggregateType).deleteEvents();
  }

  private SnapshotPolicy snapshotPolicyOf(String aggregateType) {
    requireType(aggregateType);
    return SnapshotPolicy.of(aggregateClasses.get(aggregateType));
  }
}
