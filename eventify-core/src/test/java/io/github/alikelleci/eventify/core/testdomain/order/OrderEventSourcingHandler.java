package io.github.alikelleci.eventify.core.testdomain.order;

import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.handler.annotation.MessageId;
import io.github.alikelleci.eventify.core.handler.annotation.MetadataValue;
import io.github.alikelleci.eventify.core.handler.annotation.Timestamp;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderCancelled;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderConfirmed;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderPlaced;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderShipped;

import java.time.Instant;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CORRELATION_ID;

public class OrderEventSourcingHandler {

  @EventSourcingHandler
  public Order handle(OrderPlaced event, Order state,
                     Metadata metadata,
                     @Timestamp Instant timestamp,
                     @MessageId String messageId,
                     @MetadataValue(CORRELATION_ID) String correlationId) {
    return Order.builder()
        .id(event.getId())
        .customer(event.getCustomer())
        .shippingAddress(event.getShippingAddress())
        .couponCode(event.getCouponCode())
        .status("PLACED")
        .placedAt(timestamp)
        .build();
  }

  @EventSourcingHandler
  public Order handle(OrderConfirmed event, Order state) {
    return state.toBuilder()
        .status("CONFIRMED")
        .build();
  }

  @EventSourcingHandler
  public Order handle(OrderShipped event, Order state) {
    return state.toBuilder()
        .status("SHIPPED")
        .trackingNumber(event.getTrackingNumber())
        .build();
  }

  @EventSourcingHandler
  public Order handle(OrderCancelled event, Order state) {
    return null; // aggregate removed on cancellation
  }
}
