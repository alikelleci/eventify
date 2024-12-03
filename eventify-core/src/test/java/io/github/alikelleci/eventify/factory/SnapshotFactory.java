package io.github.alikelleci.eventify.factory;

import com.github.javafaker.Faker;
import io.github.alikelleci.eventify.example.domain.Customer;
import io.github.alikelleci.eventify.messaging.eventsourcing.AggregateState;

public class SnapshotFactory {

  public static final Faker faker = new Faker();

  public static AggregateState generateSnapshotFor(String aggregateId) {
    return AggregateState.builder()
        .payload(Customer.builder()
            .id(aggregateId)
            .firstName("John")
            .lastName("Doe")
            .credits(100)
            .build())
        .build();
  }
}
