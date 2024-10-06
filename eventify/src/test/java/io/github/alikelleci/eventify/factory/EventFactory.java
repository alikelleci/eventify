package io.github.alikelleci.eventify.factory;

import com.github.javafaker.Faker;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;

import java.util.function.Consumer;

public class EventFactory {

  public static final Faker faker = new Faker();

  public static void generateEventsFor(String aggregateId, int numEvents, Consumer<Event> eventConsumer) {
    Event event;
    for (int i = 1; i <= numEvents; i++) {
      if (i == 1) {
        event = Event.builder()
            .payload(CustomerEvent.CustomerCreated.builder()
                .id(aggregateId)
                .firstName("John " + i)
                .lastName("Doe " + i)
                .credits(100)
                .build())
            .build();
      } else {
        event = Event.builder()
            .payload(CustomerEvent.CreditsAdded.builder()
                .id(aggregateId)
                .amount(1)
                .build())
            .build();
      }
      eventConsumer.accept(event);
    }
  }
}
