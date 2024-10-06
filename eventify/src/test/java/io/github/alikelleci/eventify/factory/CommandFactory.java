package io.github.alikelleci.eventify.factory;

import com.github.javafaker.Faker;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.AddCredits;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.IssueCredits;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;

import java.util.UUID;
import java.util.function.Consumer;

import static io.github.alikelleci.eventify.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.RESULT;
import static io.github.alikelleci.eventify.messaging.Metadata.TIMESTAMP;

public class CommandFactory {

  public static final Faker faker = new Faker();

  public static void generateCommandsFor(String aggregateId, int numCommands, boolean includeCreation, Consumer<Command> consumer) {
    for (int i = 1; i <= numCommands; i++) {
      Object payload;
      if (i == 1 && includeCreation) {
        payload = CreateCustomer.builder()
            .id(aggregateId)
            .firstName("John " + i)
            .lastName("Doe " + i)
            .credits(100)
            .build();
      } else {
        payload = AddCredits.builder()
            .id(aggregateId)
            .amount(1)
            .build();
      }

      consumer.accept(Command.builder()
          .payload(payload)
          .build());
    }
  }

  public static Command buildCreateCustomerCommand(String aggregateId, String firstName, String lastName, int credits) {
    return Command.builder()
        .payload(CreateCustomer.builder()
            .id(aggregateId)
            .firstName(firstName)
            .lastName(lastName)
            .credits(credits)
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

  public static Command buildAddCreditsCommand(String aggregateId, int amount) {
    return Command.builder()
        .payload(AddCredits.builder()
            .id(aggregateId)
            .amount(amount)
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

  public static Command buildIssueCreditsCommand(String aggregateId, int amount) {
    return Command.builder()
        .payload(IssueCredits.builder()
            .id(aggregateId)
            .amount(amount)
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
