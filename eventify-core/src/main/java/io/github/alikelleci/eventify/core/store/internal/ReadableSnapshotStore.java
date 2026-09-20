package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.message.internal.Revisions;
import io.github.alikelleci.eventify.core.store.SnapshotStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * Reads the snapshot store, stored by aggregate id: the latest snapshot of each aggregate.
 * {@link WritableSnapshotStore} also writes it.
 */
public class ReadableSnapshotStore implements SnapshotStore {

  private final ReadOnlyKeyValueStore<String, AggregateState> store;

  public ReadableSnapshotStore(ReadOnlyKeyValueStore<String, AggregateState> store) {
    this.store = store;
  }

  /** The aggregate's snapshot when it can be used; {@code null} when it has none, or it is outdated (see {@link #whyOutdated}). */
  @Override
  public AggregateState getUsable(String aggregateId) {
    AggregateState snapshot = get(aggregateId);
    return snapshot != null && whyOutdated(snapshot) == null ? snapshot : null;
  }

  @Override
  public AggregateState get(String aggregateId) {
    return store.get(aggregateId);
  }

  /**
   * Why the snapshot can't be used; {@code null} when it can. It can't when its aggregate can't be read (e.g. its
   * class was moved), or when it was made with another {@code @Revision} of the aggregate class: then the aggregate's
   * fields or event sourcing handlers changed, and the snapshot may hold a state the current code would not compute.
   */
  public static String whyOutdated(AggregateState snapshot) {
    if (snapshot.getPayload() == null) {
      return "its aggregate can't be read, e.g. its class was moved or a field no longer fits";
    }
    int stored = snapshot.getRevision() == 0 ? 1 : snapshot.getRevision();
    int current = Revisions.of(snapshot.getPayload().getClass());
    if (stored != current) {
      return "it was made with revision " + stored + " of " + snapshot.getPayload().getClass().getSimpleName() + ", the code is revision " + current;
    }
    return null;
  }
}
