package io.github.alikelleci.eventify.core.testdomain.order;

import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Revision;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import lombok.Builder;
import lombok.Value;

@Topic("events.order")
public interface OrderEvent {

  @Value
  @Builder
  @Revision(3)
  class OrderPlaced implements OrderEvent {
    @AggregateId
    String id;
    String customer;
    String shippingAddress; // added in revision 2
    String couponCode;      // added in revision 3
  }

  @Value
  @Builder
  class OrderConfirmed implements OrderEvent {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  class OrderShipped implements OrderEvent {
    @AggregateId
    String id;
    String trackingNumber;
  }

  @Value
  @Builder
  class OrderDelivered implements OrderEvent {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  class OrderCancelled implements OrderEvent {
    @AggregateId
    String id;
    String reason;
  }
}

