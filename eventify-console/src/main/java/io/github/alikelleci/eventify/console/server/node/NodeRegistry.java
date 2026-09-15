package io.github.alikelleci.eventify.console.server.node;

import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.rsocket.RSocket;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The application instances connected right now. An instance is here exactly as long as its connection is open, so
 * nothing needs to be stored: after a restart of the console, the instances connect again.
 */
@Slf4j
@Component
public class NodeRegistry {

  private final Map<String, ConnectedNode> nodes = new ConcurrentHashMap<>();
  private final AtomicInteger roundRobin = new AtomicInteger();

  public ConnectedNode register(NodeInfo info, RSocket rsocket) {
    ConnectedNode node = new ConnectedNode(info, rsocket, Instant.now());
    ConnectedNode previous = nodes.put(info.nodeId(), node);
    if (previous != null) {
      // The instance reconnected before its old connection timed out: the newest connection wins.
      previous.rsocket().dispose();
      log.info("Instance {} of application {} reconnected", info.nodeId(), info.applicationId());
    } else {
      log.info("Instance {} of application {} connected", info.nodeId(), info.applicationId());
    }
    return node;
  }

  public void unregister(ConnectedNode node) {
    // Only this connection: after a reconnect, the instance's entry is a newer connection that stays.
    if (nodes.remove(node.nodeId(), node)) {
      log.info("Instance {} of application {} disconnected", node.nodeId(), node.applicationId());
    }
  }

  public ConnectedNode find(String nodeId) {
    return nodeId == null ? null : nodes.get(nodeId);
  }

  /** One of the application's instances, a different one each time; {@code null} if none is connected. */
  public ConnectedNode nextNode(String applicationId) {
    List<ConnectedNode> candidates = nodes.values().stream()
        .filter(node -> node.applicationId().equals(applicationId))
        .sorted(Comparator.comparing(ConnectedNode::nodeId))
        .toList();
    if (candidates.isEmpty()) {
      return null;
    }
    return candidates.get(Math.floorMod(roundRobin.getAndIncrement(), candidates.size()));
  }

  /** All connected instances. */
  public List<ConnectedNode> nodes() {
    return List.copyOf(nodes.values());
  }
}
