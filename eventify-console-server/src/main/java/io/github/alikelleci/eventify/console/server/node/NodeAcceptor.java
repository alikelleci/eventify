package io.github.alikelleci.eventify.console.server.node;

import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.console.server.ConsoleProperties;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.exceptions.RejectedSetupException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Accepts a connecting application instance: reads who it is and keeps it in the registry while connected. */
@Slf4j
@Component
public class NodeAcceptor implements SocketAcceptor {

  private final NodeRegistry registry;
  private final JsonMapper jsonMapper;
  private final ConsoleProperties properties;

  public NodeAcceptor(NodeRegistry registry, JsonMapper jsonMapper, ConsoleProperties properties) {
    this.registry = registry;
    this.jsonMapper = jsonMapper;
    this.properties = properties;
    if (!properties.appTokenRequired()) {
      log.warn("No application token configured (eventify.console.app-token): any client that can reach the console can connect as an application.");
    }
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
    NodeInfo info;
    try {
      info = jsonMapper.readValue(setup.getDataUtf8(), NodeInfo.class);
    } catch (Exception e) {
      log.warn("Rejected a connection: unreadable instance info ({})", e.getMessage());
      return Mono.error(new RejectedSetupException("Unreadable instance info"));
    }

    if (isBlank(info.applicationId()) || isBlank(info.nodeId())) {
      log.warn("Rejected a connection without an application id or node id: {}", info);
      return Mono.error(new RejectedSetupException("applicationId and nodeId are required"));
    }
    if (properties.appTokenRequired() && !tokenMatches(setup.getMetadataUtf8())) {
      log.warn("Rejected instance {} of application {}: wrong or missing application token", info.nodeId(), info.applicationId());
      return Mono.error(new RejectedSetupException("Wrong or missing application token"));
    }
    if (info.protocolVersion() > ConsoleProtocol.VERSION) {
      log.warn("Rejected instance {} of application {}: it uses protocol version {}, this console supports up to {}",
          info.nodeId(), info.applicationId(), info.protocolVersion(), ConsoleProtocol.VERSION);
      return Mono.error(new RejectedSetupException("Protocol version " + info.protocolVersion()
          + " is not supported by this console (up to " + ConsoleProtocol.VERSION + "): upgrade the console"));
    }

    ConnectedNode node = registry.register(info, sendingSocket);
    // Closed on shutdown, on a network failure, or when the heartbeat stops (after the keepalive lifetime).
    sendingSocket.onClose()
        .doFinally(signal -> registry.unregister(node))
        .subscribe(null, e -> log.debug("Connection of instance {} closed with an error", node.nodeId(), e));

    // The console only sends requests; it answers none.
    return Mono.just(new RSocket() {});
  }

  /** Compares in constant time, so the response time doesn't reveal how much of a guess was right. */
  private boolean tokenMatches(String token) {
    return MessageDigest.isEqual(
        properties.appToken().getBytes(StandardCharsets.UTF_8),
        token.getBytes(StandardCharsets.UTF_8));
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
