package io.github.alikelleci.eventify.core.order;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.Revision;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

@TopicInfo("events.order")
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

