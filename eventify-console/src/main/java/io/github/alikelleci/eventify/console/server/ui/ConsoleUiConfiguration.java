package io.github.alikelleci.eventify.console.server.ui;

import io.github.alikelleci.eventify.console.server.ConsoleProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

/** Serves the built console UI at /console/. */
@Configuration
public class ConsoleUiConfiguration {

  private static final String CONSOLE_PATH = "/console/";

  /** Build output with a content hash in its name never changes, so it can be cached for good. */
  private static final Pattern HASHED_FILE = Pattern.compile("^(media/)?[^/]+-[A-Z0-9]{8}\\.(js|css|woff2?|ttf|eot|svg)$");

  @Bean
  public RouterFunction<ServerResponse> consoleUi(ConsoleProperties properties) {
    Path root = Path.of(properties.uiPath()).toAbsolutePath().normalize();
    return RouterFunctions.route()
        .GET("/", request -> redirectToConsole())
        .GET("/console", request -> redirectToConsole())
        .build()
        .and(RouterFunctions.resources(request -> lookup(root, request), ConsoleUiConfiguration::cacheHeaders));
  }

  private static Mono<ServerResponse> redirectToConsole() {
    // A relative location keeps the host and port the browser used.
    return ServerResponse.status(302).location(URI.create(CONSOLE_PATH)).build();
  }

  private static Mono<Resource> lookup(Path root, ServerRequest request) {
    String path = request.path();
    if (!path.startsWith(CONSOLE_PATH)) {
      return Mono.empty();
    }
    String relative = path.substring(CONSOLE_PATH.length());

    Path file = root.resolve(relative).normalize();
    if (!file.startsWith(root)) {
      return Mono.empty();
    }
    if (!relative.isEmpty() && Files.isRegularFile(file)) {
      return Mono.just(new FileSystemResource(file));
    }
    // A missing build file (e.g. from a previous version) is a real 404, not the app's HTML.
    if (HASHED_FILE.matcher(relative).matches()) {
      return Mono.empty();
    }
    // Everything else is a page of the app, like /console/aggregates/123: the app shows it.
    Path index = root.resolve("index.html");
    return Files.isRegularFile(index) ? Mono.just(new FileSystemResource(index)) : Mono.empty();
  }

  private static void cacheHeaders(Resource resource, HttpHeaders headers) {
    String name = resource.getFilename();
    if (name != null && HASHED_FILE.matcher(name).matches()) {
      headers.setCacheControl("public, max-age=31536000, immutable");
    } else {
      // index.html: the browser checks for a new version on every visit, so after an upgrade it never runs an old
      // index.html that points to files that are gone.
      headers.setCacheControl("no-cache");
    }
  }
}
