package io.github.alikelleci.eventify.console.server.node;

import io.github.alikelleci.eventify.console.plugin.ConsoleConnector;
import io.github.alikelleci.eventify.console.plugin.ConsoleRequestHandler.Reply;
import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.awaitility.Awaitility.await;

/** A console started with an application token only accepts applications that send it. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "eventify.console.app-token=secret")
class ApplicationTokenTest {

  @Value("${local.server.port}")
  int port;

  @Autowired
  NodeRegistry registry;

  final List<ConsoleConnector> connectors = new ArrayList<>();

  @AfterEach
  void tearDown() {
    connectors.forEach(ConsoleConnector::stop);
  }

  @Test
  void anApplicationWithTheTokenConnects() {
    connect("token-ok.a:0", "secret");

    await().atMost(Duration.ofSeconds(15)).until(() -> registry.find("token-ok.a:0") != null);
  }

  @Test
  void anApplicationWithAWrongOrNoTokenIsRejected() {
    connect("token-wrong.a:0", "guess");
    connect("token-missing.a:0", null);

    await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).until(() ->
        registry.find("token-wrong.a:0") == null && registry.find("token-missing.a:0") == null);
  }

  private void connect(String nodeId, String token) {
    NodeInfo info = new NodeInfo("token-test", nodeId, "localhost", "test", ConsoleProtocol.VERSION);
    ConsoleConnector connector = new ConsoleConnector(URI.create("http://localhost:" + port), token, info,
        (route, data) -> new Reply(ReplyHeader.ok(), new byte[0]));
    connector.start();
    connectors.add(connector);
  }
}
