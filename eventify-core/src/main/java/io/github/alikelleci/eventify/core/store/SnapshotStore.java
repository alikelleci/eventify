package io.github.alikelleci.eventify.core.store;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.store.internal.ReadableSnapshotStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * The latest snapshot of each aggregate of ONE aggregate type: its state after one of its events. The aggregate's
 * name belongs to the store, not to its methods.
 */
public interface SnapshotStore {

  /**
   * Reads the snapshots of one aggregate type, as Eventify stores them in a key-value store, e.g. a state store of
   * Kafka Streams.
   *
   * @param aggregateType the name of the aggregate whose snapshots are read, as its {@code @AggregateRoot} gives it
   */
  static SnapshotStore of(ReadOnlyKeyValueStore<String, AggregateState> store, String aggregateType) {
    return new ReadableSnapshotStore(store, aggregateType);
  }

  /**
   * The snapshot of the aggregate when it can be used; {@code null} when it has none, or it is outdated: made with
   * another {@code @Revision} of the aggregate, or its aggregate can't be read.
   */
  AggregateState getUsable(String aggregateId);

  /**
   * The snapshot of the aggregate as it is stored, also when it is outdated; {@code null} when it has none. An outdated
   * one can't be the start of a replay, but still tells its version: the sequence of the event it was made at. Its
   * payload is {@code null} when its aggregate can't be read.
   */
  AggregateState get(String aggregateId);
}
