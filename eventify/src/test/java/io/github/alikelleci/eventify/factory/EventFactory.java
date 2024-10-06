package io.github.alikelleci.eventify.factory;

import com.github.javafaker.Faker;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;

import java.util.function.Consumer;

public class EventFactory {

  public static final Faker faker = new Faker();

  public static void generateEventsFor(String aggregateId, int numEvents, boolean includeCreation, Consumer<Event> consumer) {
    for (int i = 1; i <= numEvents; i++) {
      Object payload;
      if (i == 1 && includeCreation) {
        payload = CustomerCreated.builder()
            .id(aggregateId)
            .firstName("John " + i)
            .lastName("Doe " + i)
            .credits(100)
            .build();
      } else {
        payload = CreditsAdded.builder()
            .id(aggregateId)
            .amount(1)
            .build();
      }
      consumer.accept(Event.builder()
          .payload(payload)
          .build());
    }
  }
}
