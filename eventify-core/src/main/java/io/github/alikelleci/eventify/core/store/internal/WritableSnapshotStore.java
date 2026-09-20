package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.store.StoreKeys;
import org.apache.kafka.streams.state.KeyValueStore;

/** The snapshot store as command handling uses it: read (see {@link ReadableSnapshotStore}) and written. */
public class WritableSnapshotStore extends ReadableSnapshotStore {

  private final KeyValueStore<String, AggregateState> store;

  public WritableSnapshotStore(KeyValueStore<String, AggregateState> store, String aggregateType) {
    super(store, aggregateType);
    this.store = store;
  }

  /** Replaces the aggregate's snapshot. */
  public void save(AggregateState state) {
    store.put(StoreKeys.snapshot(aggregateType(), state.getAggregateId()), state);
  }
}
