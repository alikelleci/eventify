package io.github.alikelleci.eventify.console.client;

import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.alikelleci.eventify.console.client.ConsoleConnector.MAX_RUNNING_QUERIES;
import static io.github.alikelleci.eventify.console.client.ConsoleConnector.MAX_WAITING_QUERIES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** The console sends more requests than the application should handle at once: the application protects itself. */
class ConsoleConnectorLimitTest {

  private final AtomicReference<RSocket> application = new AtomicReference<>();
  private final AtomicInteger running = new AtomicInteger();
  private final AtomicInteger mostRunning = new AtomicInteger();
  /** The queries keep running until the test releases them, so none finishes before all requests are in. */
  private final CountDownLatch release = new CountDownLatch(1);
  private final List<String> replies = new CopyOnWriteArrayList<>();
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

    NodeInfo info = new NodeInfo("limit-test", "limit-test.a:0", "localhost", "test", ConsoleProtocol.VERSION);
    connector = new ConsoleConnector(URI.create("http://localhost:" + port), null, info, (route, data, cancel) -> {
      mostRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        running.decrementAndGet();
      }
      return new Reply(ReplyHeader.ok(), new byte[0]);
    });
    connector.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> connector.isConnected());
  }

  @AfterEach
  void tearDown() {
    release.countDown();
    if (connector != null) connector.stop();
    if (console != null) console.dispose();
  }

  @Test
  void neverMoreQueriesRunAtOnceThanTheLimit() {
    int requests = MAX_RUNNING_QUERIES + 16;
    sendAll(requests);

    await().atMost(Duration.ofSeconds(10)).until(() -> running.get() == MAX_RUNNING_QUERIES);
    release.countDown();

    // The waiting ones run once a query finishes: all requests get their answer.
    await().atMost(Duration.ofSeconds(30)).until(() -> replies.size() == requests);
    assertThat(replies).allMatch(reply -> reply.contains("\"OK\""));
    assertThat(mostRunning).hasValue(MAX_RUNNING_QUERIES);
  }

  @Test
  void requestsBeyondTheWaitingLimitAreRefusedAsBusy() {
    int refused = 6;
    int requests = MAX_RUNNING_QUERIES + MAX_WAITING_QUERIES + refused;
    sendAll(requests);

    // The refused ones are answered right away, while the others are still running or waiting.
    await().atMost(Duration.ofSeconds(10)).until(() -> replies.size() == refused);
    assertThat(replies).allMatch(reply -> reply.contains("The application is busy right now"));

    release.countDown();
    await().atMost(Duration.ofSeconds(60)).until(() -> replies.size() == requests);
    assertThat(replies.stream().filter(reply -> reply.contains("\"OK\""))).hasSize(MAX_RUNNING_QUERIES + MAX_WAITING_QUERIES);
    assertThat(mostRunning).hasValue(MAX_RUNNING_QUERIES);
  }

  private void sendAll(int requests) {
    Flux.range(0, requests)
        .flatMap(i -> application.get().requestResponse(DefaultPayload.create(new byte[0], ConsoleConnectorCancelTest.requestHeader(Route.COMMANDS))), requests)
        .subscribe(payload -> replies.add(payload.getMetadataUtf8()));
  }
}
