package io.github.alikelleci.eventify.console.server.api;

import io.github.alikelleci.eventify.console.protocol.NodeStatus;

import java.time.Instant;
import java.util.List;

/** An application as the UI shows it: its name, and the instances connected right now with how each one is doing. */
public record ApplicationView(String name, List<NodeView> nodes) {

  /** @param status how the instance is doing, or {@code null} when it didn't answer */
  public record NodeView(String nodeId, String hostname, String version, Instant connectedAt, NodeStatus status) {
  }
}
