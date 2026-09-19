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

  /**
   * The snapshot of the aggregate when it can be used; {@code null} when it has none, or it is outdated: made with
   * another {@code @Revision} of the aggregate, or its aggregate can't be read.
   */
  AggregateState get(String aggregateId);

  /**
   * The snapshot of the aggregate as it is stored, also when it is outdated; {@code null} when it has none. An outdated
   * one can't be the start of a replay, but still tells its version: the sequence of the event it was made at. Its
   * payload is {@code null} when its aggregate can't be read.
   */
  AggregateState find(String aggregateId);
}
