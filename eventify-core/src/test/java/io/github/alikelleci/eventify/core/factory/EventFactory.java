package io.github.alikelleci.eventify.core.factory;

import com.github.javafaker.Faker;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;

import java.util.ArrayList;
import java.util.List;

public class EventFactory {

  public static final Faker faker = new Faker();

  public static List<Event> generateEventsFor(String aggregateId, int numEvents, boolean includeCreation) {
    List<Event> list = new ArrayList<>();
    for (int i = 1; i <= numEvents; i++) {
      Object payload;
      if (i == 1 && includeCreation) {
        payload = CustomerCreated.builder()
            .id(aggregateId)
            .firstName("John")
            .lastName("Doe")
            .credits(100)
            .build();
      } else {
        payload = CreditsAdded.builder()
            .id(aggregateId)
            .amount(1)
            .build();
      }
      list.add(Event.builder()
          .payload(payload)
          .build());
    }
    return list;
  }
}
