package io.github.alikelleci.eventify.console.server.security;

import io.github.alikelleci.eventify.console.server.ConsoleProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.client.oidc.web.server.logout.OidcClientInitiatedServerLogoutSuccessHandler;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrations;
import org.springframework.security.oauth2.client.registration.InMemoryReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.ReactiveClientRegistrationRepository;
import org.springframework.security.web.server.DelegatingServerAuthenticationEntryPoint;
import org.springframework.security.web.server.DelegatingServerAuthenticationEntryPoint.DelegateEntry;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.authentication.HttpStatusServerEntryPoint;
import org.springframework.security.web.server.authentication.RedirectServerAuthenticationEntryPoint;
import org.springframework.security.web.server.csrf.CookieServerCsrfTokenRepository;
import org.springframework.security.web.server.csrf.CsrfToken;
import org.springframework.security.web.server.csrf.ServerCsrfTokenRequestAttributeHandler;
import org.springframework.security.web.server.util.matcher.ServerWebExchangeMatchers;
import org.springframework.web.server.WebFilter;
import reactor.core.publisher.Mono;

import java.net.URI;

/**
 * Who may use the console. With {@code eventify.console.oidc} set, people log in with the identity provider: every page
 * and API call needs a login. Without it, the console is open to anyone who can reach it.
 *
 * <p>Applications don't log in here: they connect on the RSocket endpoint, which is outside these rules, and prove who
 * they are with the application token (see {@code NodeAcceptor}).
 */
@Slf4j
@Configuration
public class SecurityConfiguration {

  static final String REGISTRATION_ID = "sso";

  @Bean
  public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http, ConsoleProperties properties,
                                                       ObjectProvider<ReactiveClientRegistrationRepository> registrations) {
    if (!properties.loginEnabled()) {
      log.warn("No login configured (eventify.console.oidc): anyone who can reach the console can use it.");
      return http
          .authorizeExchange(exchanges -> exchanges.anyExchange().permitAll())
          .csrf(ServerHttpSecurity.CsrfSpec::disable)
          .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
          .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
          .build();
    }

    // The browser is sent to the identity provider; the API answers 401, so the UI can reload and log in again.
    DelegatingServerAuthenticationEntryPoint entryPoint = new DelegatingServerAuthenticationEntryPoint(
        new DelegateEntry(ServerWebExchangeMatchers.pathMatchers("/api/**"), new HttpStatusServerEntryPoint(HttpStatus.UNAUTHORIZED)));
    entryPoint.setDefaultEntryPoint(new RedirectServerAuthenticationEntryPoint("/oauth2/authorization/" + REGISTRATION_ID));

    // After logging out here, also log out at the identity provider (if it supports that), then back to the console.
    OidcClientInitiatedServerLogoutSuccessHandler logoutSuccessHandler = new OidcClientInitiatedServerLogoutSuccessHandler(registrations.getObject());
    logoutSuccessHandler.setPostLogoutRedirectUri("{baseUrl}/console/");
    logoutSuccessHandler.setLogoutSuccessUrl(URI.create("/console/"));

    return http
        .authorizeExchange(exchanges -> exchanges.anyExchange().authenticated())
        .oauth2Login(login -> {})
        .logout(logout -> logout.logoutSuccessHandler(logoutSuccessHandler))
        .exceptionHandling(exceptions -> exceptions.authenticationEntryPoint(entryPoint))
        // The UI sends the token from the XSRF-TOKEN cookie back in the X-XSRF-TOKEN header, as Angular does by default.
        .csrf(csrf -> csrf
            .csrfTokenRepository(CookieServerCsrfTokenRepository.withHttpOnlyFalse())
            .csrfTokenRequestHandler(new ServerCsrfTokenRequestAttributeHandler()))
        .build();
  }

  /** The identity provider, read from its discovery document when the console starts. */
  @Bean
  @ConditionalOnProperty("eventify.console.oidc.issuer-uri")
  public ReactiveClientRegistrationRepository clientRegistrations(ConsoleProperties properties) {
    ConsoleProperties.Oidc oidc = properties.oidc();
    ClientRegistration registration = ClientRegistrations.fromIssuerLocation(oidc.issuerUri())
        .registrationId(REGISTRATION_ID)
        .clientId(oidc.clientId())
        .clientSecret(oidc.clientSecret())
        .scope(oidc.scopes())
        .build();
    return new InMemoryReactiveClientRegistrationRepository(registration);
  }

  /** Spring only creates the CSRF cookie when something asks for the token; this asks on every request. */
  @Bean
  public WebFilter csrfCookieWebFilter() {
    return (exchange, chain) -> {
      Mono<CsrfToken> token = exchange.getAttributeOrDefault(CsrfToken.class.getName(), Mono.empty());
      return token.then(chain.filter(exchange));
    };
  }
}
