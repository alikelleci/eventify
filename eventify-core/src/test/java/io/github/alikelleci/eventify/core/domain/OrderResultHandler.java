package io.github.alikelleci.eventify.core.domain;

import io.github.alikelleci.eventify.core.domain.OrderCommand.CancelOrder;
import io.github.alikelleci.eventify.core.domain.OrderCommand.ConfirmOrder;
import io.github.alikelleci.eventify.core.domain.OrderCommand.DeliverOrder;
import io.github.alikelleci.eventify.core.domain.OrderCommand.PlaceOrder;
import io.github.alikelleci.eventify.core.domain.OrderCommand.ShipOrder;
import io.github.alikelleci.eventify.core.messaging.resulthandling.annotations.HandleResult;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderResultHandler {

  @HandleResult
  public void handle(PlaceOrder command) {
  }

  @HandleResult
  public void handle(ConfirmOrder command) {
  }

  @HandleResult
  public void handle(ShipOrder command) {
  }

  @HandleResult
  public void handle(DeliverOrder event) {
  }

  @HandleResult
  public void handle(CancelOrder event) {
  }

}

