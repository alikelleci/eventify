package io.github.alikelleci.eventify.core.command;

import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Scheduled")
class ScheduledTest {

  @Topic("commands.order")
  public record ExpireOrder(@AggregateId String id) {
  }

  @Test
  @DisplayName("Should hold a payload or a command with its delivery time")
  void holdsACommandAndItsTime() {
    Instant at = Instant.parse("2030-01-01T00:00:00Z");
    Command command = Command.builder().payload(new ExpireOrder("order-1")).build();

    assertThat(Scheduled.of(new ExpireOrder("order-1"), at)).isEqualTo(new Scheduled(new ExpireOrder("order-1"), at));
    assertThat(Scheduled.of(command, at).command()).isSameAs(command);
  }

  @Test
  @DisplayName("Should refuse a missing time or command, a time before 1970, and scheduling a scheduled command")
  void refusesInvalidValues() {
    Instant at = Instant.parse("2030-01-01T00:00:00Z");

    assertThatThrownBy(() -> Scheduled.of("payload", null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Scheduled.of(null, at)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Scheduled.of("payload", Instant.ofEpochMilli(-1))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Scheduled.of(Scheduled.of("payload", at), at)).isInstanceOf(IllegalArgumentException.class);
  }
}
