package io.github.alikelleci.eventify.core.support;

import com.github.javafaker.Faker;
import io.github.alikelleci.eventify.core.domain.OrderCommand.PlaceOrder;
import io.github.alikelleci.eventify.core.domain.OrderCommand.ConfirmOrder;
import io.github.alikelleci.eventify.core.domain.OrderCommand.ShipOrder;
import io.github.alikelleci.eventify.core.domain.OrderCommand.DeliverOrder;
import io.github.alikelleci.eventify.core.domain.OrderCommand.CancelOrder;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.github.alikelleci.eventify.core.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.core.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.core.messaging.Metadata.RESULT;

public class CommandFactory {

  public static final Faker faker = new Faker();

  private static Metadata defaultMetadata() {
    return Metadata.builder()
        .put("custom-key", "custom-value")
        .put(CORRELATION_ID, UUID.randomUUID().toString())
        .put(RESULT, "should-be-overwritten")
        .put(CAUSE, "should-be-overwritten")
        .build();
  }

  public static Command buildPlaceOrderCommand(String aggregateId) {
    return Command.builder()
        .payload(PlaceOrder.builder()
            .id(aggregateId)
            .customer(faker.name().fullName())
            .shippingAddress(faker.address().fullAddress())
            .build())
        .metadata(defaultMetadata())
        .build();
  }

  public static Command buildConfirmOrderCommand(String aggregateId) {
    return Command.builder()
        .payload(ConfirmOrder.builder()
            .id(aggregateId)
            .build())
        .metadata(defaultMetadata())
        .build();
  }

  public static Command buildShipOrderCommand(String aggregateId) {
    return Command.builder()
        .payload(ShipOrder.builder()
            .id(aggregateId)
            .trackingNumber(faker.number().digits(12))
            .build())
        .metadata(defaultMetadata())
        .build();
  }

  public static Command buildDeliverOrderCommand(String aggregateId) {
    return Command.builder()
        .payload(DeliverOrder.builder()
            .id(aggregateId)
            .build())
        .metadata(defaultMetadata())
        .build();
  }

  public static Command buildCancelOrderCommand(String aggregateId, String reason) {
    return Command.builder()
        .payload(CancelOrder.builder()
            .id(aggregateId)
            .reason(reason)
            .build())
        .metadata(defaultMetadata())
        .build();
  }
}