package io.github.alikelleci.eventify.core.account;

import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import jakarta.validation.ValidationException;
import lombok.Builder;
import lombok.Value;

import java.util.List;

public class AccountMessages {

  @Topic("commands.account")
  public interface AccountCommand {
  }

  @Topic("events.account")
  public interface AccountEvent {
  }

  @Value
  @Builder
  public static class OpenAccount implements AccountCommand {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class Deposit implements AccountCommand {
    @AggregateId
    String id;
    int amount;
  }

  /** Several deposits in one command: one event each. */
  @Value
  @Builder
  public static class DepositEach implements AccountCommand {
    @AggregateId
    String id;
    List<Integer> amounts;
  }

  @Value
  @Builder
  public static class AccountOpened implements AccountEvent {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class Deposited implements AccountEvent {
    @AggregateId
    String id;
    int amount;
  }

  public static class AccountHandler {

    @HandleCommand
    public AccountEvent handle(OpenAccount command, Account state) {
      if (state != null) throw new ValidationException("Account already exists.");
      return AccountOpened.builder().id(command.getId()).build();
    }

    @HandleCommand
    public AccountEvent handle(Deposit command, Account state) {
      if (state == null) throw new ValidationException("Account does not exist.");
      return Deposited.builder().id(command.getId()).amount(command.getAmount()).build();
    }

    @HandleCommand
    public List<AccountEvent> handle(DepositEach command, Account state) {
      if (state == null) throw new ValidationException("Account does not exist.");
      return command.getAmounts().stream()
          .map(amount -> (AccountEvent) Deposited.builder().id(command.getId()).amount(amount).build())
          .toList();
    }

    @ApplyEvent
    public Account apply(AccountOpened event, Account state) {
      return Account.builder().id(event.getId()).balance(0).build();
    }

    @ApplyEvent
    public Account apply(Deposited event, Account state) {
      return state.toBuilder().balance(state.getBalance() + event.getAmount()).build();
    }
  }
}
