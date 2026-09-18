package io.github.alikelleci.eventify.core.store;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.store.internal.SnapshotStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/** The latest snapshot of each aggregate: its state after one of its events. */
public interface ReadOnlySnapshotStore {

  /** Reads the snapshots of a key-value store that holds them by aggregate id, e.g. a state store of Kafka Streams. */
  static ReadOnlySnapshotStore of(ReadOnlyKeyValueStore<String, AggregateState> store) {
    return SnapshotStore.readOnly(store);
  }

  /** The snapshot of the aggregate; {@code null} when it has none. */
  AggregateState get(String aggregateId);
}
