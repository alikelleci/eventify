package io.github.alikelleci.eventify.console.server.security;

import com.sun.net.httpserver.HttpServer;
import io.github.alikelleci.eventify.console.client.ConsoleConnector;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.server.node.NodeRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockOidcLogin;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.springSecurity;

/** A console with login: people must log in with the identity provider, applications still connect without. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DisplayName("Login")
class LoginTest {

  /** Just enough of an identity provider for the console to start: its discovery document. */
  static final HttpServer identityProvider = startIdentityProvider();
  static final String ISSUER = "http://localhost:" + identityProvider.getAddress().getPort();

  @DynamicPropertySource
  static void oidc(DynamicPropertyRegistry registry) {
    registry.add("eventify.console.oidc.issuer-uri", () -> ISSUER);
    registry.add("eventify.console.oidc.client-id", () -> "eventify-console");
    registry.add("eventify.console.oidc.client-secret", () -> "client-secret");
  }

  @AfterAll
  static void stopIdentityProvider() {
    identityProvider.stop(0);
  }

  @Value("${local.server.port}")
  int port;

  @Autowired
  ApplicationContext context;

  @Autowired
  NodeRegistry registry;

  WebTestClient http() {
    return WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
  }

  /** Talks to the console as if logged in. */
  WebTestClient loggedIn() {
    return WebTestClient.bindToApplicationContext(context).apply(springSecurity()).configureClient().build()
        .mutateWith(mockOidcLogin().idToken(token -> token.claim("name", "Ada Lovelace")));
  }

  @Test
  @DisplayName("Should require a login for the API")
  void theApiNeedsALogin() {
    http().get().uri("/api/apps").exchange()
        .expectStatus().isUnauthorized();
  }

  @Test
  @DisplayName("Should send the browser to the identity provider for the UI")
  void theUiSendsTheBrowserToTheIdentityProvider() {
    http().get().uri("/console/").exchange()
        .expectStatus().isFound()
        .expectHeader().location("/oauth2/authorization/sso");

    String location = http().get().uri("/oauth2/authorization/sso").exchange()
        .expectStatus().isFound()
        .returnResult(Void.class).getResponseHeaders().getLocation().toString();
    assertThat(location).startsWith(ISSUER + "/authorize").contains("client_id=eventify-console");
  }

  @Test
  @DisplayName("Should use the address the person opened for the login when behind a proxy")
  void behindAProxyTheLoginUsesTheAddressThePersonOpened() {
    String location = http().get().uri("/oauth2/authorization/sso")
        .header("X-Forwarded-Proto", "https")
        .header("X-Forwarded-Host", "eventify-console.example.com")
        .exchange()
        .expectStatus().isFound()
        .returnResult(Void.class).getResponseHeaders().getLocation().toString();
    assertThat(location).contains("redirect_uri=https://eventify-console.example.com/login/oauth2/code/sso");
  }

  @Test
  @DisplayName("Should show a logged-in person their name and let them use the API")
  void aLoggedInPersonSeesTheirNameAndCanUseTheApi() {
    loggedIn().get().uri("/api/session").exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.loginEnabled").isEqualTo(true)
        .jsonPath("$.user").isEqualTo("Ada Lovelace");

    loggedIn().get().uri("/api/apps").exchange()
        .expectStatus().isOk();
  }

  @Test
  @DisplayName("Should require the CSRF token to retry a command")
  void aRetryNeedsTheCsrfToken() {
    loggedIn().post().uri("/api/apps/orders/commands/retry")
        .header("Content-Type", "application/json").bodyValue("{}")
        .exchange()
        .expectStatus().isForbidden();

    // With the token it gets past the security checks; no application is connected, so the console can't retry.
    loggedIn().mutateWith(csrf()).post().uri("/api/apps/orders/commands/retry")
        .header("Content-Type", "application/json").bodyValue("{}")
        .exchange()
        .expectStatus().isEqualTo(503);
  }

  @Test
  @DisplayName("Should let applications connect without a login")
  void applicationsConnectWithoutALogin() {
    NodeInfo info = new NodeInfo("login-test", "login-test.a:0", "localhost", "test", ConsoleProtocol.VERSION);
    ConsoleConnector connector = new ConsoleConnector(URI.create("http://localhost:" + port), null, info,
        (route, data, cancel) -> new Reply(ReplyHeader.ok(), new byte[0]));
    connector.start();
    try {
      await().atMost(Duration.ofSeconds(15)).until(() -> registry.find("login-test.a:0") != null);
    } finally {
      connector.stop();
    }
  }

  private static HttpServer startIdentityProvider() {
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      String issuer = "http://localhost:" + server.getAddress().getPort();
      String discovery = """
          {"issuer":"%1$s","authorization_endpoint":"%1$s/authorize","token_endpoint":"%1$s/token",
           "jwks_uri":"%1$s/jwks","userinfo_endpoint":"%1$s/userinfo","end_session_endpoint":"%1$s/logout",
           "response_types_supported":["code"],"subject_types_supported":["public"],
           "id_token_signing_alg_values_supported":["RS256"]}""".formatted(issuer);
      server.createContext("/.well-known/openid-configuration", exchange -> {
        byte[] body = discovery.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(body);
        }
      });
      server.start();
      return server;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
