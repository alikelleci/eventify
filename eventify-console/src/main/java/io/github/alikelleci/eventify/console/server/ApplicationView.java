package io.github.alikelleci.eventify.console.server;

import java.time.Instant;
import java.util.List;

/** An application as the UI shows it: its name and the instances connected right now. */
public record ApplicationView(String name, List<NodeView> nodes) {

  public record NodeView(String nodeId, String hostname, String version, Instant connectedAt) {
  }
}
