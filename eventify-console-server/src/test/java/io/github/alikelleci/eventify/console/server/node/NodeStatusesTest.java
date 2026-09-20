package io.github.alikelleci.eventify.console.server.node;

import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.console.protocol.NodeStatus;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("Node statuses")
class NodeStatusesTest {

  private final NodeGateway gateway = mock(NodeGateway.class);
  private final NodeStatuses statuses = new NodeStatuses(gateway, JsonMapper.builder().build());
  private final ConnectedNode node = new ConnectedNode(
      new NodeInfo("app", "app.a:0", "localhost", "test", ConsoleProtocol.VERSION, Set.of("order")), null, Instant.now());

  @Test
  @DisplayName("Should not cancel the answer for another page when one page stops waiting")
  void aPageThatStopsWaitingDoesNotCancelTheAnswerForAnother() throws Exception {
    AtomicInteger asked = new AtomicInteger();
    byte[] running = "{\"state\":\"RUNNING\",\"stateForMs\":1,\"restoring\":false}".getBytes(StandardCharsets.UTF_8);
    // An instance that takes a moment to answer.
    when(gateway.sendTo(eq(node), eq(Route.STATUS), any())).thenReturn(Mono.defer(() -> {
      asked.incrementAndGet();
      return Mono.just(new Reply(ReplyHeader.ok(), running)).delayElement(Duration.ofMillis(500));
    }));

    // Two pages ask at the same time; one of them refreshes before the answer is there.
    Disposable refreshed = statuses.of(node).subscribe();
    CompletableFuture<Optional<NodeStatus>> other = statuses.of(node).toFuture();
    refreshed.dispose();

    assertThat(other.get(5, TimeUnit.SECONDS)).map(NodeStatus::state).contains("RUNNING");
    assertThat(asked).hasValue(1);
  }
}
