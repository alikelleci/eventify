package io.github.alikelleci.eventify.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class StateStoreNameResolver {

  public String resolveEventStoreName(Class<?> aggregateType) {
    return aggregateType.getSimpleName() + "-event-store";
  }

  public String resolveSnapshotStoreName(Class<?> aggregateType) {
    return aggregateType.getSimpleName() + "-snapshot-store";
  }
}
