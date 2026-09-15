package io.github.alikelleci.eventify.console.server;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param uiPath         the folder with the built console UI (its index.html), served at /console/
 * @param requestTimeout how long to wait for an application to answer; reading commands from Kafka can take a while
 */
@ConfigurationProperties("eventify.console")
public record ConsoleProperties(
    @DefaultValue("./ui") String uiPath,
    @DefaultValue("60s") Duration requestTimeout) {
}
