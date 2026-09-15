package io.github.alikelleci.eventify.console.server;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param requestTimeout how long to wait for an application to answer; reading commands from Kafka can take a while
 * @param appToken       the secret applications must send when they connect; without it, any client that can reach
 *                       the console can connect as an application
 */
@ConfigurationProperties("eventify.console")
public record ConsoleProperties(
    @DefaultValue("60s") Duration requestTimeout,
    String appToken) {

  public boolean appTokenRequired() {
    return appToken != null && !appToken.isBlank();
  }
}
