package io.github.alikelleci.eventify.console.plugin.item;

import io.github.alikelleci.eventify.console.plugin.item.ItemCommand.CreateItem;
import io.github.alikelleci.eventify.console.plugin.item.ItemEvent.ItemCreated;
import io.github.alikelleci.eventify.core.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.annotations.ApplyEvent;

public class ItemHandler {

  @HandleCommand
  public ItemEvent handle(CreateItem command, Item state) {
    return ItemCreated.builder().id(command.getId()).name(command.getName()).build();
  }

  @ApplyEvent
  public Item apply(ItemCreated event, Item state) {
    return Item.builder().id(event.getId()).name(event.getName()).build();
  }
}
