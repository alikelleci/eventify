package io.github.alikelleci.eventify.console.client;

import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** How the connector behaves when the console goes away, comes back, or refuses it. */
@DisplayName("Console connector: reconnecting")
class ConsoleConnectorReconnectTest {

  private final AtomicInteger setups = new AtomicInteger();
  private final List<RSocket> connections = new CopyOnWriteArrayList<>();
  private volatile boolean rejecting;
  private CloseableChannel console;
  private ConsoleConnector connector;

  @AfterEach
  void tearDown() {
    if (connector != null) connector.stop();
    if (console != null) stopConsole();
  }

  @Test
  @DisplayName("Should reconnect when the console comes back")
  void reconnectsWhenTheConsoleComesBack() throws Exception {
    int port = freePort();
    console = startConsole(port);

    NodeInfo info = new NodeInfo("reconnect-test", "reconnect-test.a:0", "localhost", "test", ConsoleProtocol.VERSION);
    connector = new ConsoleConnector(URI.create("http://localhost:" + port), null, info,
        (route, data, cancel) -> Reply.of(ReplyHeader.ok()));
    connector.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> connector.isConnected() && setups.get() == 1);

    // The console goes down, like a redeploy: the process ends, which closes its connections too...
    stopConsole();
    await().atMost(Duration.ofSeconds(10)).until(() -> !connector.isConnected());

    // ...stays down for a while, so the connector has to retry...
    Thread.sleep(3000);

    // ...and comes back on the same address.
    console = startConsole(port);
    await().atMost(Duration.ofSeconds(45)).until(() -> connector.isConnected() && setups.get() == 2);
  }

  @Test
  @DisplayName("Should try again slowly when the console rejects the instance")
  void aRejectedInstanceTriesAgainSlowly() throws Exception {
    int port = freePort();
    rejecting = true;
    console = startConsole(port);

    NodeInfo info = new NodeInfo("reject-test", "reject-test.a:0", "localhost", "test", ConsoleProtocol.VERSION);
    connector = new ConsoleConnector(URI.create("http://localhost:" + port), null, info,
        (route, data, cancel) -> Reply.of(ReplyHeader.ok()));
    connector.start();

    // One attempt, then the next one only after the long delay: not a new connection every second.
    await().atMost(Duration.ofSeconds(10)).until(() -> setups.get() == 1);
    Thread.sleep(5000);
    assertThat(setups.get()).isEqualTo(1);
    assertThat(connector.isConnected()).isFalse();
  }

  private CloseableChannel startConsole(int port) {
    return RSocketServer.create(SocketAcceptor.with(new RSocket() {}))
        .acceptor((setup, sendingSocket) -> {
          setups.incrementAndGet();
          if (rejecting) {
            return Mono.error(new RejectedSetupException("Protocol version 2 is not supported by this console (up to 1): upgrade the console"));
          }
          connections.add(sendingSocket);
          return Mono.just(new RSocket() {});
        })
        .bind(WebsocketServerTransport.create("localhost", port))
        .block(Duration.ofSeconds(10));
  }

  private void stopConsole() {
    console.dispose();
    console.onClose().block(Duration.ofSeconds(10));
    connections.forEach(RSocket::dispose);
    connections.clear();
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
