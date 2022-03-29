package io.github.alikelleci.eventify.example.handlers;

import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsIssued;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.LastNameChanged;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.annotations.HandleEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomerEventHandler {

  @HandleEvent
  public void handle(CustomerCreated event, Metadata metadata) {
  }

  @HandleEvent
  public void handle(FirstNameChanged event) {
  }

  @HandleEvent
  public void handle(LastNameChanged event) {
  }

  @HandleEvent
  public void handle(CreditsAdded event) {
  }

  @HandleEvent
  public void handle(CreditsIssued event) {
  }

  @HandleEvent
  public void handle(CustomerDeleted event) {
  }

}

