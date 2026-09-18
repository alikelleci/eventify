package io.github.alikelleci.eventify.console.client.item;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AggregateRoot
public class Item {
  @AggregateId
  String id;
  String name;
}
