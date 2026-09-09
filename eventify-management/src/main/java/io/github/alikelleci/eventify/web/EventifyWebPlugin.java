package io.github.alikelleci.eventify.web;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.plugin.EventifyPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.net.URI;

@Slf4j
public class EventifyWebPlugin implements EventifyPlugin {

  private EventifyManagementServer managementServer;

  @Override
  public void onStart(Eventify eventify) {
    String applicationServer = eventify.getStreamsConfig()
        .getProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, "");

    if (applicationServer.isBlank()) {
      log.warn("'{}' is not configured, Eventify management server will not start.",
          StreamsConfig.APPLICATION_SERVER_CONFIG);
      return;
    }

    int port = URI.create("http://" + applicationServer).getPort();
    EventifyQueryService queryService = new EventifyQueryService(eventify);
    managementServer = new EventifyManagementServer(queryService, eventify.getObjectMapper(), port);

    try {
      managementServer.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start Eventify management server", e);
    }
  }

  @Override
  public void onStop(Eventify eventify) {
    if (managementServer != null) {
      managementServer.stop();
    }
  }
}
