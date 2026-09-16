package io.github.alikelleci.eventify.console.server;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

/**
 * @param requestTimeout how long to wait for an application to answer; reading commands from Kafka can take a while
 * @param appToken       the secret applications must send when they connect; without it, any client that can reach
 *                       the console can connect as an application
 * @param oidc           login with an OpenID Connect identity provider; without it, the console needs no login
 */
@ConfigurationProperties("eventify.console")
public record ConsoleProperties(
    @DefaultValue("60s") Duration requestTimeout,
    String appToken,
    Oidc oidc) {

  /**
   * @param issuerUri    the identity provider, e.g. https://login.example.com/realms/ops
   * @param clientId     the console's client id at the identity provider
   * @param clientSecret the console's client secret at the identity provider
   * @param scopes       the scopes to ask for
   */
  public record Oidc(String issuerUri, String clientId, String clientSecret,
                     @DefaultValue({"openid", "profile", "email"}) List<String> scopes) {
  }

  public boolean appTokenRequired() {
    return appToken != null && !appToken.isBlank();
  }

  public boolean loginEnabled() {
    return oidc != null && oidc.issuerUri() != null && !oidc.issuerUri().isBlank();
  }
}
