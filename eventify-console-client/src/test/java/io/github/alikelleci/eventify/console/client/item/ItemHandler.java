package io.github.alikelleci.eventify.console.client.item;

import io.github.alikelleci.eventify.console.client.item.ItemCommand.CreateItem;
import io.github.alikelleci.eventify.console.client.item.ItemEvent.ItemCreated;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;

public class ItemHandler {

  @CommandHandler
  public ItemEvent handle(CreateItem command, Item state) {
    return ItemCreated.builder().id(command.getId()).name(command.getName()).build();
  }

  @EventSourcingHandler
  public Item handle(ItemCreated event, Item state) {
    return Item.builder().id(event.getId()).name(event.getName()).build();
  }
}
