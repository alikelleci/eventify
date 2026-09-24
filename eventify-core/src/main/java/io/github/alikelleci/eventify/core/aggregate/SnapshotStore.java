package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/** The latest aggregate state saved for every aggregate type and id. */
public final class SnapshotStore {

  private final ReadOnlyKeyValueStore<String, AggregateState> readable;
  /** Present only on the command-processing thread. */
  private final KeyValueStore<String, AggregateState> writable;

  public SnapshotStore(ReadOnlyKeyValueStore<String, AggregateState> store) {
    this.readable = store;
    this.writable = store instanceof KeyValueStore<String, AggregateState> keyValueStore ? keyValueStore : null;
  }

  /** The stored snapshot, including an outdated one; {@code null} when none exists. */
  public AggregateState get(String aggregateType, String aggregateId) {
    return readable.get(StoreKeys.aggregate(aggregateType, aggregateId));
  }

  /** Replaces the snapshot. Only command processing may call this. */
  public void save(String aggregateType, AggregateState state) {
    if (writable == null) {
      throw new IllegalStateException("This SnapshotStore is read-only.");
    }
    writable.put(StoreKeys.aggregate(aggregateType, state.getAggregateId()), state);
  }
}
