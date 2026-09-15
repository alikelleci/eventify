package io.github.alikelleci.eventify.console.server;

import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.exceptions.RejectedSetupException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/** Accepts a connecting application instance: reads who it is and keeps it in the registry while connected. */
@Slf4j
@Component
@RequiredArgsConstructor
public class NodeAcceptor implements SocketAcceptor {

  private final NodeRegistry registry;
  private final JsonMapper jsonMapper;

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

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
