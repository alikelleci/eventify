package io.github.alikelleci.eventify.console.plugin.item;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

@TopicInfo("commands.item")
public interface ItemCommand {

  @Value
  @Builder
  class CreateItem implements ItemCommand {
    @AggregateId
    String id;
    String name;
  }
}
