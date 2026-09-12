package io.github.alikelleci.eventify.console;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.plugin.EventifyPlugin;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.net.URI;

@Slf4j
public class EventifyConsolePlugin implements EventifyPlugin {

  private final String allowedOrigins;
  private EventifyConsoleServer consoleServer;
  private EventifyService queryService;

  @Builder
  private EventifyConsolePlugin(String allowedOrigins) {
    this.allowedOrigins = allowedOrigins != null ? allowedOrigins : "*";
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
    queryService = new EventifyService(eventify);
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
    if (queryService != null) {
      queryService.close();
    }
  }
}
