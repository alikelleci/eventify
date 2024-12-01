package io.github.alikelleci.eventify.example.handlers;

import io.github.alikelleci.eventify.common.annotations.MessageId;
import io.github.alikelleci.eventify.common.annotations.MetadataValue;
import io.github.alikelleci.eventify.common.annotations.Timestamp;
import io.github.alikelleci.eventify.example.domain.Customer;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsIssued;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.LastNameChanged;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventsourcing.annotations.ApplyEvent;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;

@Slf4j
public class CustomerEventSourcingHandler {

  @ApplyEvent
  public Customer handle(Customer state,
                         CustomerCreated event,
                         Metadata metadata,
                         @Timestamp Instant timestamp,
                         @MessageId String messageId,
                         @MetadataValue(CORRELATION_ID) String correlationId) {
    return Customer.builder()
        .id(event.getId())
        .firstName(event.getFirstName())
        .lastName(event.getLastName())
        .credits(event.getCredits())
        .birthday(event.getBirthday())
        .dateCreated(timestamp)
        .build();
  }

  @ApplyEvent
  public Customer handle(Customer state, FirstNameChanged event) {
    return state.toBuilder()
        .firstName(event.getFirstName())
        .build();
  }

  @ApplyEvent
  public Customer handle(Customer state, LastNameChanged event) {
    return state.toBuilder()
        .lastName(event.getLastName())
        .build();
  }

  @ApplyEvent
  public Customer handle(Customer state, CreditsAdded event) {
    return state.toBuilder()
        .credits(state.getCredits() + event.getAmount())
        .build();
  }

  @ApplyEvent
  public Customer handle(Customer state, CreditsIssued event) {
    return state.toBuilder()
        .credits(state.getCredits() - event.getAmount())
        .build();
  }

  @ApplyEvent
  public Customer handle(Customer state, CustomerDeleted event) {
    return null;
  }
}
