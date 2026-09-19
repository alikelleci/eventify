package io.github.alikelleci.eventify.core.command.gateway;

import io.github.alikelleci.eventify.core.EventifyException;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.command.exception.CommandExecutionException;
import io.github.alikelleci.eventify.core.command.exception.CommandTimeoutException;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** What sendAndWait returns and throws, for each way the future of a command can end. */
@DisplayName("Command gateway: sendAndWait")
class SendAndWaitTest {

  record Ship(@AggregateId String id) {
  }

  private final Command command = Command.builder().payload(new Ship("order-1")).build();

  @Test
  @DisplayName("Should return the result")
  void returnsTheResult() {
    CommandResult.Success success = new CommandResult.Success(command, List.of());

    assertThat(answering(CompletableFuture.completedFuture(success)).sendAndWait(command)).isSameAs(success);
  }

  @Test
  @DisplayName("Should throw the CommandExecutionException of a failed command itself, not wrapped")
  void throwsTheFailureItself() {
    CommandExecutionException failure = new CommandExecutionException("Order cannot be shipped.");

    assertThatThrownBy(() -> answering(CompletableFuture.failedFuture(failure)).sendAndWait(command))
        .isSameAs(failure);
  }

  @Test
  @DisplayName("Should throw CommandTimeoutException when no result arrives in time")
  void throwsATimeoutWhenWaitingTooLong() {
    assertThatThrownBy(() -> answering(new CompletableFuture<>()).sendAndWait(command, 10, TimeUnit.MILLISECONDS))
        .isInstanceOf(CommandTimeoutException.class)
        .hasMessageContaining(command.getId());
  }

  @Test
  @DisplayName("Should throw CommandTimeoutException when the gateway stopped waiting for the result")
  void throwsATimeoutWhenTheGatewayStoppedWaiting() {
    CompletableFuture<CommandResult.Success> timedOut = CompletableFuture.failedFuture(new TimeoutException("Command timed out"));

    assertThatThrownBy(() -> answering(timedOut).sendAndWait(command))
        .isInstanceOf(CommandTimeoutException.class);
  }

  @Test
  @DisplayName("Should keep the interrupt flag when the wait is interrupted")
  void keepsTheInterruptFlag() {
    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> answering(new CompletableFuture<>()).sendAndWait(command))
          .isInstanceOf(EventifyException.class)
          .hasMessageContaining("Interrupted");
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted(); // cleared, for the next test on this thread
    }
  }

  private static CommandGateway answering(CompletableFuture<CommandResult.Success> future) {
    return new CommandGateway() {
      @Override
      public CompletableFuture<CommandResult.Success> send(Command command) {
        return future;
      }

      @Override
      public void close() {
      }
    };
  }
}
