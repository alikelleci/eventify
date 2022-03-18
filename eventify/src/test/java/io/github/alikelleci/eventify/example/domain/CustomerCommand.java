package io.github.alikelleci.eventify.example.domain;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

import javax.validation.constraints.Max;
import javax.validation.constraints.NotBlank;
import java.time.Instant;

@TopicInfo("commands.customer")
public interface CustomerCommand {

  @Value
  @Builder
  class CreateCustomer implements CustomerCommand {
    @AggregateId
    private String id;
    @NotBlank
    private String firstName;
    @NotBlank
    private String lastName;
    @Max(100)
    private int credits;
    private Instant birthday;
  }

  @Value
  @Builder
  class ChangeFirstName implements CustomerCommand {
    @AggregateId
    private String id;
    @NotBlank
    private String firstName;
  }

  @Value
  @Builder
  class ChangeLastName implements CustomerCommand {
    @AggregateId
    private String id;
    @NotBlank
    private String lastName;
  }

  @Value
  @Builder
  class AddCredits implements CustomerCommand {
    @AggregateId
    private String id;
    private int amount;
  }

  @Value
  @Builder
  class IssueCredits implements CustomerCommand {
    @AggregateId
    private String id;
    private int amount;
  }

  @Value
  @Builder
  class DeleteCustomer implements CustomerCommand {
    @AggregateId
    private String id;
  }
}
