package io.github.alikelleci.eventify.factory;

import com.github.javafaker.Faker;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.AddCredits;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.IssueCredits;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.github.alikelleci.eventify.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.RESULT;

public class CommandFactory {

  public static final Faker faker = new Faker();

  public static List<Command> generateCommandsFor(String aggregateId, int numCommands, boolean includeCreation) {
    List<Command> list = new ArrayList<>();
    for (int i = 1; i <= numCommands; i++) {
      Object payload;
      if (i == 1 && includeCreation) {
        payload = CreateCustomer.builder()
            .id(aggregateId)
            .firstName("John")
            .lastName("Doe")
            .credits(100)
            .build();
      } else {
        payload = AddCredits.builder()
            .id(aggregateId)
            .amount(1)
            .build();
      }
      list.add(Command.builder()
          .payload(payload)
          .build());
    }
    return list;
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
            .put("custom-key", "custom-value")
            .put(CORRELATION_ID, UUID.randomUUID().toString())
            .put(RESULT, "should-be-overwritten")
            .put(CAUSE, "should-be-overwritten")
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
            .put("custom-key", "custom-value")
            .put(CORRELATION_ID, UUID.randomUUID().toString())
            .put(RESULT, "should-be-overwritten")
            .put(CAUSE, "should-be-overwritten")
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
            .put("custom-key", "custom-value")
            .put(CORRELATION_ID, UUID.randomUUID().toString())
            .put(RESULT, "should-be-overwritten")
            .put(CAUSE, "should-be-overwritten")
            .build())
        .build();
  }
}
