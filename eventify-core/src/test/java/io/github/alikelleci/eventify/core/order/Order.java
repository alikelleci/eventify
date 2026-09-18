package io.github.alikelleci.eventify.core.order;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder(toBuilder = true)
@AggregateRoot
@EnableSnapshotting(threshold = 3)
public class Order {
  @AggregateId
  String id;
  String customer;
  String shippingAddress;
  String couponCode;
  String status;
  String trackingNumber;
  Instant placedAt;
}

