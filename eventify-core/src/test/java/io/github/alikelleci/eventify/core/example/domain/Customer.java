package io.github.alikelleci.eventify.core.example.domain;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.AggregateRoot;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.AggregateRoot;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder(toBuilder = true)
@AggregateRoot
public class Customer {
  @AggregateId
  private String id;
  private String firstName;
  private String lastName;
  private int credits;
  private Instant birthday;
  private Instant dateCreated;
}
