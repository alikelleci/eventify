package io.github.alikelleci.eventify.core.order;

import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import jakarta.validation.constraints.NotBlank;
import lombok.Builder;
import lombok.Value;

@Topic("commands.order")
public interface OrderCommand {

  @Value
  @Builder
  class PlaceOrder implements OrderCommand {
    @AggregateId
    String id;
    @NotBlank
    String customer;
    @NotBlank
    String shippingAddress;
    String couponCode;
  }

  @Value
  @Builder
  class ConfirmOrder implements OrderCommand {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  class ShipOrder implements OrderCommand {
    @AggregateId
    String id;
    @NotBlank
    String trackingNumber;
  }

  @Value
  @Builder
  class DeliverOrder implements OrderCommand {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  class CancelOrder implements OrderCommand {
    @AggregateId
    String id;
    @NotBlank
    String reason;
  }
}

