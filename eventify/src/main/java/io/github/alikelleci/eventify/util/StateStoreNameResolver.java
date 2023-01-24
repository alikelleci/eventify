package io.github.alikelleci.eventify.util;

public class StateStoreNameResolver {

  public static String resolveEventStoreName(Class<?> aggregateType) {
    return aggregateType.getSimpleName() + "-event-store";
  }

  public static String resolveSnapshotStoreName(Class<?> aggregateType) {
    return aggregateType.getSimpleName() + "-snapshot-store";
  }
}
