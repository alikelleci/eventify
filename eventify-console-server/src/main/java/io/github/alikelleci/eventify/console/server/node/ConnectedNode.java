package io.github.alikelleci.eventify.console.server.node;

import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.rsocket.RSocket;

import java.time.Instant;

/** An application instance with an open connection to the console. */
public record ConnectedNode(NodeInfo info, RSocket rsocket, Instant connectedAt) {

  public String nodeId() {
    return info.nodeId();
  }

  public String applicationId() {
    return info.applicationId();
  }

  /** What tells instances apart: two applications can have an instance with the same node id. */
  public Key key() {
    return new Key(applicationId(), nodeId());
  }

  public record Key(String applicationId, String nodeId) {
  }
}
