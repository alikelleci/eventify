package io.github.alikelleci.eventify.core.testdomain.order;

import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.MessageId;
import io.github.alikelleci.eventify.core.message.annotation.MetadataValue;
import io.github.alikelleci.eventify.core.message.annotation.Timestamp;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderCancelled;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderConfirmed;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderDelivered;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderPlaced;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderShipped;

import java.time.Instant;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CORRELATION_ID;

public class OrderEventSourcingHandler {

  @ApplyEvent
  public Order apply(OrderPlaced event, Order state,
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

  @ApplyEvent
  public Order apply(OrderConfirmed event, Order state) {
    return state.toBuilder()
        .status("CONFIRMED")
        .build();
  }

  @ApplyEvent
  public Order apply(OrderShipped event, Order state) {
    return state.toBuilder()
        .status("SHIPPED")
        .trackingNumber(event.getTrackingNumber())
        .build();
  }

  @ApplyEvent
  public Order apply(OrderDelivered event, Order state) {
    return state.toBuilder()
        .status("DELIVERED")
        .build();
  }

  @ApplyEvent
  public Order apply(OrderCancelled event, Order state) {
    return null; // aggregate removed on cancellation
  }
}
