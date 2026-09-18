package io.github.alikelleci.eventify.console.client;

import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.plugin.EventifyPlugin;
import lombok.Builder;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.net.InetAddress;
import java.net.URI;

/** Connects the application to the Eventify Console, so it can be inspected there. */
public class EventifyConsoleClient implements EventifyPlugin {

  private final URI url;
  private final String token;
  private ConsoleService consoleService;
  /** Follows the state changes and restorations, so the status can be answered without asking Kafka. */
  private final StatusTracker statusTracker = new StatusTracker();
  private ConsoleConnector connector;

  /**
   * @param url   the console's address, as opened in the browser, e.g. {@code http://localhost:8080}
   * @param token the console's application token; only needed when the console is started with one
   */
  @Builder
  private EventifyConsoleClient(String url, String token) {
    if (url == null || url.isBlank()) {
      throw new IllegalArgumentException("The Eventify Console url is required");
    }
    this.url = URI.create(url.trim());
    this.token = token;
  }

  @Override
  public void onStart(Eventify eventify) {
    NodeInfo nodeInfo = new NodeInfo(
        eventify.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_ID_CONFIG),
        ConsoleService.nodeId(ConsoleService.hostInfo(eventify)),
        hostname(),
        EventifyConsoleClient.class.getPackage().getImplementationVersion(),
        ConsoleProtocol.VERSION);

    consoleService = new ConsoleService(eventify, statusTracker);
    ConsoleRequestHandler handler = new ConsoleRequestHandler(consoleService, eventify.getObjectMapper());
    connector = new ConsoleConnector(url, token, nodeInfo, handler::handle);
    connector.start();
  }

  @Override
  public StateListener stateListener() {
    return statusTracker;
  }

  @Override
  public StateRestoreListener stateRestoreListener() {
    return statusTracker;
  }

  @Override
  public void onStop(Eventify eventify) {
    if (connector != null) {
      connector.stop();
      connector = null;
    }
    if (consoleService != null) {
      consoleService.close();
      consoleService = null;
    }
  }

  private static String hostname() {
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
}
