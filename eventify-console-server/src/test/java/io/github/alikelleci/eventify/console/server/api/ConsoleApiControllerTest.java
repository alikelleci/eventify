package io.github.alikelleci.eventify.console.server.api;

import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Console API responses")
class ConsoleApiControllerTest {

  /** E.g. an aggregate whose stored events have a gap: the console shows why, and doesn't offer to try again. */
  @Test
  @DisplayName("Should answer data that can't be read with 422 and the reason")
  void unreadableDataIs422WithTheReason() {
    ResponseEntity<byte[]> response = ConsoleApiController.toResponse(Reply.of(ReplyHeader.unreadable("expected #12, found #15")));

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
    assertThat(new String(response.getBody(), StandardCharsets.UTF_8)).isEqualTo("expected #12, found #15");
  }
}
