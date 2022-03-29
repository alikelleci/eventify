package io.github.alikelleci.eventify.example.handlers;

import io.github.alikelleci.eventify.example.domain.CustomerCommand.AddCredits;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.ChangeFirstName;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.ChangeLastName;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.DeleteCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.IssueCredits;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.resulthandling.annotations.HandleResult;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomerResultHandler {

  @HandleResult
  public void handle(CreateCustomer command, Metadata metadata) {
  }

  @HandleResult
  public void handle(ChangeFirstName command) {
  }

  @HandleResult
  public void handle(ChangeLastName command) {
  }

  @HandleResult
  public void handle(AddCredits event) {
  }

  @HandleResult
  public void handle(IssueCredits event) {
  }

  @HandleResult
  public void handle(DeleteCustomer event) {
  }

}

