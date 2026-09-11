package io.github.alikelleci.eventify.console;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.plugin.EventifyPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.net.URI;

@Slf4j
public class EventifyConsolePlugin implements EventifyPlugin {

  private EventifyConsoleServer consoleServer;
  private final String allowedOrigins;

  public EventifyConsolePlugin() {
    this("*");
  }

  public EventifyConsolePlugin(String allowedOrigins) {
    this.allowedOrigins = allowedOrigins;
  }

  @Override
  public void onStart(Eventify eventify) {
    String applicationServer = eventify.getStreamsConfig()
        .getProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, "");

    if (applicationServer.isBlank()) {
      log.warn("'{}' is not configured, Eventify console server will not start.",
          StreamsConfig.APPLICATION_SERVER_CONFIG);
      return;
    }

    int port = URI.create("http://" + applicationServer).getPort();
    EventifyQueryService queryService = new EventifyQueryService(eventify);
    consoleServer = new EventifyConsoleServer(queryService, eventify.getObjectMapper(), port, allowedOrigins);

    try {
      consoleServer.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start Eventify console server", e);
    }
  }

  @Override
  public void onStop(Eventify eventify) {
    if (consoleServer != null) {
      consoleServer.stop();
    }
  }
}
