package io.github.alikelleci.eventify.console.server.api;

import java.time.Instant;
import java.util.List;

/** An application as the UI shows it: its name, the instances connected right now, and how it is doing. */
public record ApplicationView(String name, List<NodeView> nodes, ApplicationStatusView status) {

  public record NodeView(String nodeId, String hostname, String version, Instant connectedAt) {
  }
}
