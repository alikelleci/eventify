package io.github.alikelleci.eventify.console.server.security;

import io.github.alikelleci.eventify.console.server.ConsoleProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** What the UI needs to know about the console and the person using it. */
@RestController
@RequiredArgsConstructor
public class SessionController {

  private final ConsoleProperties properties;

  /**
   * @param loginEnabled     whether people log in; the UI then shows who is logged in and a log out button
   * @param user             the logged-in person's name, or {@code null} without login
   * @param appTokenRequired whether applications need the application token to connect
   */
  public record SessionView(boolean loginEnabled, String user, boolean appTokenRequired) {
  }

  @GetMapping("/api/session")
  public SessionView session(@AuthenticationPrincipal OidcUser user) {
    return new SessionView(properties.loginEnabled(), user == null ? null : displayName(user), properties.appTokenRequired());
  }

  private static String displayName(OidcUser user) {
    if (user.getFullName() != null) return user.getFullName();
    if (user.getPreferredUsername() != null) return user.getPreferredUsername();
    return user.getName();
  }
}
