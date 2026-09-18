package io.github.alikelleci.eventify.console.client.item;

import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import lombok.Builder;
import lombok.Value;

@Topic("commands.item")
public interface ItemCommand {

  @Value
  @Builder
  class CreateItem implements ItemCommand {
    @AggregateId
    String id;
    String name;
  }
}
