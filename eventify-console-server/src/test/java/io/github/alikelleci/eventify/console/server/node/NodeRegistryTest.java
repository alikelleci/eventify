package io.github.alikelleci.eventify.console.server.node;

import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.rsocket.RSocket;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Node registry")
class NodeRegistryTest {

  private final NodeRegistry registry = new NodeRegistry();

  /** Two applications can set the same {@code application.server}, e.g. both localhost:8080. */
  @Test
  @DisplayName("Should keep the instances of two applications apart when they have the same node id")
  void twoApplicationsWithTheSameNodeIdBothStayConnected() {
    Connection orders = new Connection();
    Connection payments = new Connection();

    registry.register(info("orders", "localhost:8080"), orders);
    registry.register(info("payments", "localhost:8080"), payments);

    assertThat(orders.closed).as("connection of orders closed").isFalse();
    assertThat(registry.nodesOf("orders")).extracting(ConnectedNode::rsocket).containsExactly(orders);
    assertThat(registry.nodesOf("payments")).extracting(ConnectedNode::rsocket).containsExactly(payments);
  }

  @Test
  @DisplayName("Should replace the connection of an instance that connects again")
  void anInstanceThatConnectsAgainReplacesItsOldConnection() {
    Connection old = new Connection();
    Connection current = new Connection();

    ConnectedNode first = registry.register(info("orders", "localhost:8080"), old);
    registry.register(info("orders", "localhost:8080"), current);
    registry.unregister(first); // the old connection closes afterwards

    assertThat(old.closed).as("old connection closed").isTrue();
    assertThat(registry.nodesOf("orders")).extracting(ConnectedNode::rsocket).containsExactly(current);
  }

  private static NodeInfo info(String applicationId, String nodeId) {
    return new NodeInfo(applicationId, nodeId, "host", "test", ConsoleProtocol.VERSION, Set.of("thing"));
  }

  private static class Connection implements RSocket {
    volatile boolean closed;

    @Override
    public void dispose() {
      closed = true;
    }
  }
}
