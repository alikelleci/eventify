package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.store.ReadOnlySnapshotStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/** The snapshot store, stored by aggregate id: the latest snapshot of each aggregate. */
public class SnapshotStore implements ReadOnlySnapshotStore {

  private final ReadOnlyKeyValueStore<String, AggregateState> reads;
  /** {@code null} when read-only. */
  private final KeyValueStore<String, AggregateState> writes;

  public SnapshotStore(KeyValueStore<String, AggregateState> store) {
    this(store, store);
  }

  private SnapshotStore(ReadOnlyKeyValueStore<String, AggregateState> reads, KeyValueStore<String, AggregateState> writes) {
    this.reads = reads;
    this.writes = writes;
  }

  public static ReadOnlySnapshotStore readOnly(ReadOnlyKeyValueStore<String, AggregateState> store) {
    return new SnapshotStore(store, null);
  }

  @Override
  public AggregateState get(String aggregateId) {
    return reads.get(aggregateId);
  }

  /** Replaces the aggregate's snapshot. */
  public void save(AggregateState state) {
    writes.put(state.getAggregateId(), state);
  }
}
