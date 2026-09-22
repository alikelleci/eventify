package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.aggregate.internal.AggregateTypes;
import io.github.alikelleci.eventify.core.aggregate.internal.SnapshotPolicy;
import io.github.alikelleci.eventify.core.message.internal.Revisions;

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

  /** Why this snapshot cannot rebuild this aggregate type, or {@code null} when it can. */
  public String whySnapshotIsOutdated(String aggregateType, AggregateState snapshot) {
    // A removed state deliberately has no payload type. A type without a payload instead came from a snapshot whose
    // aggregate could not be deserialized and must not silently become a deletion.
    if (snapshot.getPayload() == null && snapshot.getType() != null) {
      return "its aggregate can't be read, e.g. its class was moved or a field no longer fits";
    }
    String wrongPayload = whyPayloadDoesNotMatch(aggregateType, snapshot.getPayload());
    if (wrongPayload != null) {
      return wrongPayload;
    }
    // Checked for a removed state too: a later revision of the event sourcing handlers may not remove it.
    int stored = snapshot.getRevision() == 0 ? 1 : snapshot.getRevision();
    Class<?> aggregateClass = aggregateClassOf(aggregateType);
    int current = Revisions.of(aggregateClass);
    if (stored != current) {
      return "it was made with revision " + stored + " of " + aggregateClass.getSimpleName() + ", the code is revision " + current;
    }
    return null;
  }

  /** The {@link io.github.alikelleci.eventify.core.message.annotation.Revision} of this aggregate type's class. */
  public int revisionOf(String aggregateType) {
    return Revisions.of(aggregateClassOf(aggregateType));
  }

  /** Why a payload cannot be the state of this aggregate type, or {@code null} when it can. */
  public String whyPayloadDoesNotMatch(String aggregateType, Object payload) {
    if (payload == null) {
      return null;
    }
    Class<?> aggregateClass = aggregateClassOf(aggregateType);
    if (payload.getClass() != aggregateClass) {
      return "its aggregate is " + payload.getClass().getName() + ", but " + aggregateType + " requires " + aggregateClass.getName();
    }
    return null;
  }

  private SnapshotPolicy snapshotPolicyOf(String aggregateType) {
    return SnapshotPolicy.of(aggregateClassOf(aggregateType));
  }

  private Class<?> aggregateClassOf(String aggregateType) {
    requireType(aggregateType);
    return aggregateClasses.get(aggregateType);
  }
}
