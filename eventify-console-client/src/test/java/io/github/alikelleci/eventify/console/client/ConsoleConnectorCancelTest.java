package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.RequestHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** The console cancels a request while its query runs, e.g. because someone refreshed the page. */
class ConsoleConnectorCancelTest {

  private final AtomicReference<RSocket> application = new AtomicReference<>();
  /** What each query saw, by the name it was sent with. */
  private final Map<String, String> outcomes = new ConcurrentHashMap<>();
  private CloseableChannel console;
  private ConsoleConnector connector;

  @BeforeEach
  void setUp() throws Exception {
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    console = RSocketServer.create((setup, sendingSocket) -> {
          application.set(sendingSocket);
          return Mono.just(new RSocket() {});
        })
        .bind(WebsocketServerTransport.create("localhost", port))
        .block(Duration.ofSeconds(10));

    NodeInfo info = new NodeInfo("cancel-test", "cancel-test.a:0", "localhost", "test", ConsoleProtocol.VERSION);
    connector = new ConsoleConnector(URI.create("http://localhost:" + port), null, info, (route, data, cancel) -> {
      String name = new String(data, StandardCharsets.UTF_8);
      try {
        // A slow query, like reading commands from Kafka, that stops as soon as it's told to.
        long until = System.currentTimeMillis() + 3000;
        while (!cancel.isCancelled() && System.currentTimeMillis() < until) {
          Thread.sleep(20);
        }
        outcomes.put(name, cancel.isCancelled() ? "stopped" : "finished");
      } catch (InterruptedException e) {
        outcomes.put(name, "interrupted");
      }
      return new Reply(ReplyHeader.ok(), name.getBytes(StandardCharsets.UTF_8));
    });
    connector.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> connector.isConnected());
  }

  @AfterEach
  void tearDown() {
    if (connector != null) connector.stop();
    if (console != null) console.dispose();
  }

  @Test
  void aRunningQueryIsToldToStopAndNotInterrupted() {
    Disposable request = send("slow").subscribe();
    await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(1)).until(() -> true);
    request.dispose();

    await().atMost(Duration.ofSeconds(2)).until(() -> outcomes.containsKey("slow"));
    assertThat(outcomes.get("slow")).isEqualTo("stopped");
  }

  @Test
  void cancellingOneRequestDoesNotAffectAnother() {
    Disposable cancelled = send("tab-1").subscribe();
    Mono<String> other = send("tab-2").map(payload -> payload.getDataUtf8());

    await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(1)).until(() -> true);
    cancelled.dispose();

    assertThat(other.block(Duration.ofSeconds(10))).isEqualTo("tab-2");
    assertThat(outcomes.get("tab-2")).isEqualTo("finished");
    await().atMost(Duration.ofSeconds(2)).until(() -> "stopped".equals(outcomes.get("tab-1")));
  }

  private Mono<Payload> send(String name) {
    return application.get().requestResponse(DefaultPayload.create(
        name.getBytes(StandardCharsets.UTF_8), requestHeader(Route.COMMANDS)));
  }

  static byte[] requestHeader(Route route) {
    try {
      return new ObjectMapper().writeValueAsBytes(RequestHeader.of(route));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
