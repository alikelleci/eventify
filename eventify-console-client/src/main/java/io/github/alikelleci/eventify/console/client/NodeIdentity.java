package io.github.alikelleci.eventify.console.client;

import io.github.alikelleci.eventify.core.plugin.PluginContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;

import java.net.InetAddress;

/** Who this node is: the name it has among the application's nodes, and where it runs. */
final class NodeIdentity {

  private NodeIdentity() {
  }

  /** This instance's {@code application.server}: the application's own, or the one Eventify sets. */
  static HostInfo hostInfo(PluginContext eventify) {
    return HostInfo.buildFromEndpoint(eventify.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_SERVER_CONFIG));
  }

  /** How instances refer to each other: {@link #hostInfo(PluginContext)} as {@code host:port}. */
  static String nodeId(HostInfo hostInfo) {
    return hostInfo.host() + ":" + hostInfo.port();
  }

  /** Where this node runs, for display: the container's hostname, or else the machine's. */
  static String hostname() {
    // Set in containers (the pod name in Kubernetes); looking it up can be slow on some machines.
    String hostname = System.getenv("HOSTNAME");
    if (hostname != null && !hostname.isBlank()) {
      return hostname;
    }
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return null;
    }
  }

  /** The client id of a Kafka client the console adds to this node, e.g. {@code orders-console-producer}. */
  static String clientId(PluginContext eventify, String purpose) {
    return eventify.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-console-" + purpose;
  }
}
