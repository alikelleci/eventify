package io.github.alikelleci.eventify.core.account;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.AggregateRoot;
import io.github.alikelleci.eventify.core.common.annotations.EnableSnapshotting;
import lombok.Builder;
import lombok.Value;

/** An aggregate that deletes its events at every second event, when a snapshot is taken. */
@Value
@Builder(toBuilder = true)
@AggregateRoot
@EnableSnapshotting(threshold = 2, deleteEvents = true)
public class Account {
  @AggregateId
  String id;
  int balance;
}
