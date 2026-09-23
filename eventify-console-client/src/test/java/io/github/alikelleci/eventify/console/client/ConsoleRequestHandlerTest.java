package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Requests the handler refuses before it queries anything: no service is needed for them. */
@DisplayName("Console request handler: bad requests")
class ConsoleRequestHandlerTest {

  private final ConsoleRequestHandler handler = new ConsoleRequestHandler(null, new ObjectMapper(), Set.of("order", "invoice"));

  @Test
  @DisplayName("Should refuse a page of events whose cursor is not a sequence")
  void refusesACursorThatIsNotASequence() {
    Reply reply = handle(Route.EVENTS, "{\"aggregateType\":\"order\",\"aggregateId\":\"order-1\",\"cursor\":0}");

    assertThat(reply.header().status()).isEqualTo(ReplyHeader.Status.BAD_REQUEST);
    assertThat(reply.header().reason()).contains("cursor");
    assertThat(handle(Route.EVENTS, "{\"aggregateType\":\"order\",\"aggregateId\":\"order-1\",\"cursor\":\"abc\"}").header().status())
        .isEqualTo(ReplyHeader.Status.BAD_REQUEST);
  }

  @Test
  @DisplayName("Should refuse an event without a sequence")
  void refusesAnEventWithoutASequence() {
    assertThat(handle(Route.EVENT_DETAIL, "{\"aggregateType\":\"order\",\"aggregateId\":\"order-1\"}").header().status())
        .isEqualTo(ReplyHeader.Status.BAD_REQUEST);
    assertThat(handle(Route.EVENT_DETAIL, "{\"aggregateType\":\"order\",\"aggregateId\":\"order-1\",\"sequence\":0}").header().reason())
        .contains("sequence");
  }

  /** An aggregate is addressed by its type and its identifier: without the type there is no aggregate to look in. */
  @Test
  @DisplayName("Should refuse a request without an aggregate type")
  void refusesARequestWithoutAnAggregateType() {
    Reply reply = handle(Route.EVENTS, "{\"aggregateId\":\"order-1\"}");

    assertThat(reply.header().status()).isEqualTo(ReplyHeader.Status.BAD_REQUEST);
    assertThat(reply.header().reason()).contains("aggregateType");
  }

  /**
   * A name this instance doesn't handle reads an empty store, which would look like an aggregate without a history.
   * Refused instead, so a name that is spelled wrong is not mistaken for an aggregate that has nothing.
   */
  @Test
  @DisplayName("Should refuse an aggregate this instance does not handle")
  void refusesAnAggregateThisInstanceDoesNotHandle() {
    Reply reply = handle(Route.EVENTS, "{\"aggregateType\":\"odrer\",\"aggregateId\":\"order-1\"}");

    assertThat(reply.header().status()).isEqualTo(ReplyHeader.Status.BAD_REQUEST);
    assertThat(reply.header().reason()).contains("odrer");
    // The other routes ask the same question about the same aggregate, so they refuse it too.
    assertThat(handle(Route.COMMANDS, "{\"aggregateType\":\"odrer\",\"aggregateId\":\"order-1\"}").header().status())
        .isEqualTo(ReplyHeader.Status.BAD_REQUEST);
    assertThat(handle(Route.STATE, "{\"aggregateType\":\"odrer\",\"aggregateId\":\"order-1\"}").header().status())
        .isEqualTo(ReplyHeader.Status.BAD_REQUEST);
  }

  @Test
  @DisplayName("Should refuse an aggregate id with a NUL character")
  void refusesAnAggregateIdWithANulCharacter() {
    Reply reply = handle(Route.EVENTS, "{\"aggregateType\":\"order\",\"aggregateId\":\"order\\u00001\"}");

    assertThat(reply.header().status()).isEqualTo(ReplyHeader.Status.BAD_REQUEST);
    assertThat(reply.header().reason()).contains("aggregateId");
  }

  private Reply handle(Route route, String json) {
    return handler.handle(route.name(), json.getBytes(StandardCharsets.UTF_8), new CancelSignal());
  }
}
