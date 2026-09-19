package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.client.ConsoleViews.CommandView;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** A command as the UI gets it: its own fields, with how its handling ended next to them. */
@DisplayName("Command view")
class CommandViewTest {

  record ShipOrder(@AggregateId String id) {
  }

  private final ObjectMapper objectMapper = EventifyObjectMapper.create();
  private final Command command = Command.builder().payload(new ShipOrder("order-1")).build();

  @Test
  @DisplayName("Should show a failed command with its fields, status and cause")
  void aFailure() {
    JsonNode json = objectMapper.valueToTree(CommandView.of(new CommandResult.Failure(command, "Order cannot be shipped.")));

    assertThat(json.path("id").asText()).isEqualTo(command.getId());
    assertThat(json.path("type").asText()).isEqualTo("ShipOrder");
    assertThat(json.path("payload").path("id").asText()).isEqualTo("order-1");
    assertThat(json.path("status").asText()).isEqualTo("failure");
    assertThat(json.path("cause").asText()).isEqualTo("Order cannot be shipped.");
    assertThat(json.has("command")).isFalse();
  }

  @Test
  @DisplayName("Should show a successful command without a cause")
  void aSuccess() {
    JsonNode json = objectMapper.valueToTree(CommandView.of(new CommandResult.Success(command, List.of())));

    assertThat(json.path("status").asText()).isEqualTo("success");
    assertThat(json.has("cause")).isFalse();
  }

  @Test
  @DisplayName("Should read back as the command, as a retry from the UI sends it")
  void readsBackAsTheCommand() throws Exception {
    byte[] json = objectMapper.writeValueAsBytes(CommandView.of(new CommandResult.Failure(command, "Order cannot be shipped.")));

    Command read = objectMapper.readValue(json, Command.class);

    assertThat(read.getId()).isEqualTo(command.getId());
    assertThat(read.getPayload()).isEqualTo(command.getPayload());
  }
}
