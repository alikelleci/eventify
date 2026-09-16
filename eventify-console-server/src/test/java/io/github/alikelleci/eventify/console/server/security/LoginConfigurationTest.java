package io.github.alikelleci.eventify.console.server.security;

import io.github.alikelleci.eventify.console.server.ConsoleProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Whether login is enabled is decided once, the same way everywhere, and an incomplete login fails at startup. */
class LoginConfigurationTest {

  private final ApplicationContextRunner context = new ApplicationContextRunner().withUserConfiguration(OnlyWithLogin.class);

  @Test
  void anIssuerEnablesLogin() {
    context.withPropertyValues(
            "eventify.console.oidc.issuer-uri=https://login.example.com",
            "eventify.console.oidc.client-id=console",
            "eventify.console.oidc.client-secret=secret")
        .run(started -> assertThat(started).hasBean("loginOnly"));
  }

  @Test
  void anEmptyIssuerIsNoLogin() {
    // An environment variable that is set, but empty: EVENTIFY_CONSOLE_OIDC_ISSUERURI=
    context.withPropertyValues("eventify.console.oidc.issuer-uri=")
        .run(started -> assertThat(started).hasNotFailed().doesNotHaveBean("loginOnly"));
    assertThat(new ConsoleProperties(null, null, new ConsoleProperties.Oidc(" ", null, null, List.of())).loginEnabled()).isFalse();
  }

  @Test
  void aLoginWithoutClientIdOrSecretFailsAtStartup() {
    assertThatThrownBy(() -> new ConsoleProperties(null, null, new ConsoleProperties.Oidc("https://login.example.com", null, "secret", List.of())))
        .hasMessageContaining("EVENTIFY_CONSOLE_OIDC_CLIENTID");
    assertThatThrownBy(() -> new ConsoleProperties(null, null, new ConsoleProperties.Oidc("https://login.example.com", "console", "", List.of())))
        .hasMessageContaining("EVENTIFY_CONSOLE_OIDC_CLIENTSECRET");
  }

  @Configuration
  static class OnlyWithLogin {
    @Bean
    @Conditional(SecurityConfiguration.LoginEnabled.class)
    String loginOnly() {
      return "login";
    }
  }
}
