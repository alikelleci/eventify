package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Requests the handler refuses before it queries anything: no service is needed for them. */
@DisplayName("Console request handler: bad requests")
class ConsoleRequestHandlerTest {

  private final ConsoleRequestHandler handler = new ConsoleRequestHandler(null, new ObjectMapper());

  @Test
  @DisplayName("Should refuse a page of events whose cursor is not a ULID")
  void refusesAMalformedCursor() {
    Reply reply = handle(Route.EVENTS, "{\"aggregateId\":\"order-1\",\"cursor\":\"not-a-ulid\"}");

    assertThat(reply.header().status()).isEqualTo(ReplyHeader.Status.BAD_REQUEST);
    assertThat(reply.header().reason()).contains("cursor");
  }

  @Test
  @DisplayName("Should refuse an event that is not of the aggregate")
  void refusesAnEventOfAnotherAggregate() {
    Reply reply = handle(Route.EVENT_DETAIL, "{\"aggregateId\":\"order-1\",\"eventId\":\"order-2@01J00000000000000000000000\"}");

    assertThat(reply.header().status()).isEqualTo(ReplyHeader.Status.BAD_REQUEST);
  }

  private Reply handle(Route route, String json) {
    return handler.handle(route.name(), json.getBytes(StandardCharsets.UTF_8), new CancelSignal());
  }
}
