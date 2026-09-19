package io.github.alikelleci.eventify.core.store.internal;

import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import org.apache.kafka.streams.state.KeyValueStore;

/** The snapshot store as command handling uses it: read (see {@link SnapshotStoreReader}) and written. */
public class SnapshotStore extends SnapshotStoreReader {

  private final KeyValueStore<String, AggregateState> store;

  public SnapshotStore(KeyValueStore<String, AggregateState> store) {
    super(store);
    this.store = store;
  }

  /** Replaces the aggregate's snapshot. */
  public void save(AggregateState state) {
    store.put(state.getAggregateId(), state);
  }
}
