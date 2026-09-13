package io.github.alikelleci.eventify.core.order;

import io.github.alikelleci.eventify.core.order.OrderEvent.OrderCancelled;
import io.github.alikelleci.eventify.core.order.OrderEvent.OrderConfirmed;
import io.github.alikelleci.eventify.core.order.OrderEvent.OrderDelivered;
import io.github.alikelleci.eventify.core.order.OrderEvent.OrderPlaced;
import io.github.alikelleci.eventify.core.order.OrderEvent.OrderShipped;
import io.github.alikelleci.eventify.core.messaging.eventhandling.annotations.HandleEvent;

public class OrderEventHandler {

  @HandleEvent
  public void on(OrderPlaced event) { /* insert into read model */ }

  @HandleEvent
  public void on(OrderConfirmed event) { /* update read model */ }

  @HandleEvent
  public void on(OrderShipped event) { /* send shipping notification */ }

  @HandleEvent
  public void on(OrderDelivered event) { /* update read model */ }

  @HandleEvent
  public void on(OrderCancelled event) { /* remove from read model */ }
}

