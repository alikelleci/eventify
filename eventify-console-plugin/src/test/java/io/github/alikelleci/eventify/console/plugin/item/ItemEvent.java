package io.github.alikelleci.eventify.console.plugin.item;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

@TopicInfo("events.item")
public interface ItemEvent {

  @Value
  @Builder
  class ItemCreated implements ItemEvent {
    @AggregateId
    String id;
    String name;
  }
}
