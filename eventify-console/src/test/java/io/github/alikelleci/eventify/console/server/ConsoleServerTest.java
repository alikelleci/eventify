package io.github.alikelleci.eventify.console.server;

import io.github.alikelleci.eventify.console.plugin.ConsoleConnector;
import io.github.alikelleci.eventify.console.plugin.ConsoleRequestHandler.Reply;
import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** The console against the real connector the applications use, with fake answers instead of Kafka Streams. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConsoleServerTest {

  @Value("${local.server.port}")
  int port;

  private static final String RUNNING = "{\"state\":\"RUNNING\",\"stateForMs\":5000,\"restoring\":false}";

  WebTestClient client;
  final List<ConsoleConnector> connectors = new ArrayList<>();
  /** Requests per instance, by node id. Not the status requests: listing the applications asks for those. */
  final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();

  @BeforeEach
  void setUp() {
    client = WebTestClient.bindToServer()
        .baseUrl("http://localhost:" + port)
        .responseTimeout(Duration.ofSeconds(30))
        .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
        .build();
  }

  @AfterEach
  void tearDown() {
    connectors.forEach(ConsoleConnector::stop);
  }

  @Test
  void connectedInstancesAreListedPerApplication() {
    connect("listing", "listing.a:0", ok("{}"));
    connect("listing", "listing.b:0", ok("{}"));

    awaitInstances("listing", 2);
    client.get().uri("/api/apps").exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$[?(@.name == 'listing')].nodes[*].nodeId").isEqualTo(List.of("listing.a:0", "listing.b:0"));
  }

  @Test
  void aQueryGoesToTheOwnerAfterARedirect() {
    connect("redirect", "redirect.a:0", notOwner("redirect.b:0"));
    connect("redirect", "redirect.b:0", ok("{\"events\":[],\"nextCursor\":null}"));
    awaitInstances("redirect", 2);

    for (int i = 0; i < 3; i++) {
      client.get().uri("/api/apps/redirect/aggregates/order-1/events").exchange()
          .expectStatus().isOk()
          .expectBody().json("{\"events\":[],\"nextCursor\":null}");
    }
    // The owner is remembered, so at most the first request went to the wrong instance.
    assertThat(calls("redirect.a:0")).isLessThanOrEqualTo(1);
    assertThat(calls("redirect.b:0")).isEqualTo(3);
  }

  @Test
  void anOwnerThatIsNotConnectedMeansTryAgain() {
    connect("rebalance", "rebalance.a:0", notOwner("rebalance.gone:0"));
    awaitInstances("rebalance", 1);

    client.get().uri("/api/apps/rebalance/aggregates/order-1/events").exchange()
        .expectStatus().isEqualTo(503);
  }

  @Test
  void aQueryForAnyInstanceGoesToExactlyOne() {
    connect("any", "any.a:0", ok("{\"commands\":[]}"));
    connect("any", "any.b:0", ok("{\"commands\":[]}"));
    awaitInstances("any", 2);

    client.get().uri("/api/apps/any/aggregates/order-1/commands").exchange()
        .expectStatus().isOk();
    assertThat(calls("any.a:0") + calls("any.b:0")).isEqualTo(1);
  }

  @Test
  void aRetryPassesTheCommandOnUnread() {
    Map<String, String> received = new ConcurrentHashMap<>();
    connect("retry", "retry.a:0", (route, data, cancel) -> {
      received.put(route, new String(data, StandardCharsets.UTF_8));
      return new Reply(ReplyHeader.ok(), new byte[0]);
    });
    awaitInstances("retry", 1);

    String command = "{\"id\":\"order-1@1\",\"payload\":{\"@class\":\"com.example.PlaceOrder\"}}";
    client.post().uri("/api/apps/retry/aggregates/order-1/commands/order-1@1/retry")
        .header("Content-Type", "application/json")
        .bodyValue(command)
        .exchange()
        .expectStatus().isOk();
    assertThat(received).containsEntry(Route.RETRY_COMMAND.name(), command);
  }

  @Test
  void largeRepliesArriveComplete() {
    String large = "{\"text\":\"" + "x".repeat(2_000_000) + "\"}";
    connect("large", "large.a:0", ok(large));
    awaitInstances("large", 1);

    byte[] body = client.get().uri("/api/apps/large/aggregates/order-1/events").exchange()
        .expectStatus().isOk()
        .expectBody().returnResult().getResponseBody();
    assertThat(new String(body, StandardCharsets.UTF_8)).isEqualTo(large);
  }

  @Test
  void anInstanceThatStopsIsRemoved() {
    ConsoleConnector connector = connect("stopping", "stopping.a:0", ok("{}"));
    awaitInstances("stopping", 1);

    connector.stop();
    awaitInstances("stopping", 0);
    client.get().uri("/api/apps/stopping/aggregates/order-1/events").exchange()
        .expectStatus().isEqualTo(503);
  }

  @Test
  void theApplicationsAreListedWithTheStatusOfEachInstance() {
    connect("status", "status.a:0", status("{\"state\":\"REBALANCING\",\"stateForMs\":240000,\"restoring\":true}"));
    connect("status", "status.b:0", status("{\"state\":\"RUNNING\",\"stateForMs\":5000,\"restoring\":false}"));
    connect("status", "status.c:0", (route, data, cancel) -> new Reply(ReplyHeader.unavailable("busy"), new byte[0]));
    awaitInstances("status", 3);

    // A status is kept for a moment, so it can still be from before the instance answered.
    await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> client.get().uri("/api/apps").exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$[?(@.name == 'status')].nodes[0].status.state").isEqualTo("REBALANCING")
        .jsonPath("$[?(@.name == 'status')].nodes[0].status.stateForMs").isEqualTo(240000)
        .jsonPath("$[?(@.name == 'status')].nodes[0].status.restoring").isEqualTo(true)
        .jsonPath("$[?(@.name == 'status')].nodes[1].status.state").isEqualTo("RUNNING")
        .jsonPath("$[?(@.name == 'status')].nodes[2].nodeId").isEqualTo("status.c:0")
        .jsonPath("$[?(@.name == 'status')].nodes[2].status")                // didn't answer
        .value(status -> assertThat(status).asInstanceOf(InstanceOfAssertFactories.LIST).containsExactly((Object) null)));
  }

  @Test
  void theUiIsServedForItsPages() {
    // No UI folder in this test: the redirect still works, and an unknown asset is a 404.
    client.get().uri("/").exchange()
        .expectStatus().isFound()
        .expectHeader().location("/console/");
    client.get().uri("/console/main-ABCDEFGH.js").exchange()
        .expectStatus().isNotFound();
  }

  private ConsoleConnector connect(String applicationId, String nodeId, ConsoleConnector.Handler handler) {
    NodeInfo info = new NodeInfo(applicationId, nodeId, "localhost", "test", ConsoleProtocol.VERSION);
    ConsoleConnector connector = new ConsoleConnector(URI.create("http://localhost:" + port), null, info, (route, data, cancel) -> {
      if (!route.equals(Route.STATUS.name())) {
        calls.computeIfAbsent(nodeId, id -> new AtomicInteger()).incrementAndGet();
      }
      return handler.handle(route, data, cancel);
    });
    connector.start();
    connectors.add(connector);
    return connector;
  }

  private int calls(String nodeId) {
    return calls.getOrDefault(nodeId, new AtomicInteger()).get();
  }

  private void awaitInstances(String applicationId, int count) {
    await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
        client.get().uri("/api/apps").exchange()
            .expectStatus().isOk()
            .expectBody()
            .jsonPath("$[?(@.name == '" + applicationId + "')].nodes.length()")
            .value(value -> assertThat(count == 0 ? String.valueOf(value) : value.toString()).isEqualTo(count == 0 ? "[]" : "[" + count + "]")));
  }

  /** Answers every request with this JSON; asked for its status, it is simply running. */
  private static ConsoleConnector.Handler ok(String json) {
    return (route, data, cancel) -> new Reply(ReplyHeader.ok(),
        (route.equals(Route.STATUS.name()) ? RUNNING : json).getBytes(StandardCharsets.UTF_8));
  }

  /** Answers every request, the status included, with this status. */
  private static ConsoleConnector.Handler status(String json) {
    return (route, data, cancel) -> new Reply(ReplyHeader.ok(), json.getBytes(StandardCharsets.UTF_8));
  }

  private static ConsoleConnector.Handler notOwner(String owner) {
    return (route, data, cancel) -> new Reply(ReplyHeader.notOwner(owner), new byte[0]);
  }
}
