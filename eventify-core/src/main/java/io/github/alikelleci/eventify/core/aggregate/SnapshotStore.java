package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.message.internal.Revisions;
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
    return readable.get(StoreKeys.snapshot(aggregateType, aggregateId));
  }

  /** A snapshot that can start a replay. */
  public AggregateState getUsable(String aggregateType, String aggregateId) {
    AggregateState snapshot = get(aggregateType, aggregateId);
    return snapshot != null && whyOutdated(snapshot) == null ? snapshot : null;
  }

  /** Replaces the snapshot. Only command processing may call this. */
  public void save(String aggregateType, AggregateState state) {
    if (writable == null) {
      throw new IllegalStateException("This SnapshotStore is read-only.");
    }
    writable.put(StoreKeys.snapshot(aggregateType, state.getAggregateId()), state);
  }

  /**
   * Why a snapshot cannot be used, or null when it can. An unreadable payload or a different aggregate revision can
   * represent a state the current event sourcing handlers would not compute. A deliberately removed payload remains
   * a valid checkpoint; its envelope has no type, unlike a snapshot whose payload could not be deserialized.
   */
  public static String whyOutdated(AggregateState snapshot) {
    if (snapshot.getPayload() == null) {
      // A removed state has no aggregate type. A type without a payload came from a snapshot whose payload no longer
      // deserializes, and must not silently become a deletion.
      return snapshot.getType() == null ? null : "its aggregate can't be read, e.g. its class was moved or a field no longer fits";
    }
    int stored = snapshot.getRevision() == 0 ? 1 : snapshot.getRevision();
    int current = Revisions.of(snapshot.getPayload().getClass());
    if (stored != current) {
      return "it was made with revision " + stored + " of " + snapshot.getPayload().getClass().getSimpleName() + ", the code is revision " + current;
    }
    return null;
  }
}
