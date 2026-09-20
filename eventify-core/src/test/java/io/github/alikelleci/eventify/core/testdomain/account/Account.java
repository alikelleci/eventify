package io.github.alikelleci.eventify.core.testdomain.account;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import lombok.Builder;
import lombok.Value;

/** An aggregate that deletes its events at every second event, when a snapshot is taken. */
@Value
@Builder(toBuilder = true)
@AggregateRoot("account")
@EnableSnapshotting(threshold = 2, deleteEvents = true)
public class Account {
  @AggregateId
  String id;
  int balance;
}
