package io.github.alikelleci.eventify.factory;

import com.github.javafaker.Faker;
import io.github.alikelleci.eventify.example.domain.CustomerCommand;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;

import java.util.UUID;

import static io.github.alikelleci.eventify.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.RESULT;
import static io.github.alikelleci.eventify.messaging.Metadata.TIMESTAMP;

public class CommandFactory {

  public static final Faker faker = new Faker();


  public static Command buildCreateCustomerCommand(String aggregateId) {
    return Command.builder()
        .payload(CustomerCommand.CreateCustomer.builder()
            .id(aggregateId)
            .firstName(faker.name().firstName())
            .lastName(faker.name().lastName())
            .credits(faker.number().numberBetween(50, 100))
            .birthday(faker.date().birthday(20, 60).toInstant())
            .build())
        .metadata(Metadata.builder()
            .add("custom-key", "custom-value")
            .add(CORRELATION_ID, UUID.randomUUID().toString())
            .add(ID, "should-be-overwritten")
            .add(TIMESTAMP, "should-be-overwritten")
            .add(RESULT, "should-be-overwritten")
            .add(CAUSE, "should-be-overwritten")
            .build())
        .build();
  }

  public static Command buildAddCreditsCommand(String aggregateId) {
    return Command.builder()
        .payload(CustomerCommand.AddCredits.builder()
            .id(aggregateId)
            .amount(faker.number().numberBetween(50, 100))
            .build())
        .metadata(Metadata.builder()
            .add("custom-key", "custom-value")
            .add(CORRELATION_ID, UUID.randomUUID().toString())
            .add(ID, "should-be-overwritten")
            .add(TIMESTAMP, "should-be-overwritten")
            .add(RESULT, "should-be-overwritten")
            .add(CAUSE, "should-be-overwritten")
            .build())
        .build();
  }
}
