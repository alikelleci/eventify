package io.github.alikelleci.eventify.console.client.item;

import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import lombok.Builder;
import lombok.Value;

@Topic("events.item")
public interface ItemEvent {

  @Value
  @Builder
  class ItemCreated implements ItemEvent {
    @AggregateId
    String id;
    String name;
  }
}
