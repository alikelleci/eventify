package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/** Stores the latest snapshot for each aggregate. */
public final class SnapshotStore {

  private final ReadOnlyKeyValueStore<String, AggregateState> readable;
  /** Available only while processing commands. */
  private final KeyValueStore<String, AggregateState> writable;

  public SnapshotStore(ReadOnlyKeyValueStore<String, AggregateState> store) {
    this.readable = store;
    this.writable = store instanceof KeyValueStore<String, AggregateState> keyValueStore ? keyValueStore : null;
  }

  /** Returns the snapshot, or {@code null}. */
  public AggregateState get(String aggregateType, String aggregateId) {
    return readable.get(StoreKeys.aggregate(aggregateType, aggregateId));
  }

  /** Saves a snapshot. */
  public void save(String aggregateType, AggregateState state) {
    if (writable == null) {
      throw new IllegalStateException("This SnapshotStore is read-only.");
    }
    writable.put(StoreKeys.aggregate(aggregateType, state.getAggregateId()), state);
  }
}
