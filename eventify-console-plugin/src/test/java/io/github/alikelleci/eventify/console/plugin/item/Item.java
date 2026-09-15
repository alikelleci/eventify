package io.github.alikelleci.eventify.console.plugin.item;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.AggregateRoot;
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
