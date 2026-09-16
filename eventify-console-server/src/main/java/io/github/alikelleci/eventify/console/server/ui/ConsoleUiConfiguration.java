package io.github.alikelleci.eventify.console.server.ui;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.regex.Pattern;

/** Serves the console UI at /console/. The Maven build puts it in the jar, under console-ui/. */
@Configuration
public class ConsoleUiConfiguration {

  private static final String CONSOLE_PATH = "/console/";
  private static final String UI_LOCATION = "console-ui/";

  /** Build output with a content hash in its name never changes, so it can be cached for good. */
  private static final Pattern HASHED_FILE = Pattern.compile("^(media/)?[^/]+-[A-Z0-9]{8}\\.(js|css|woff2?|ttf|eot|svg)$");

  @Bean
  public RouterFunction<ServerResponse> consoleUi() {
    return RouterFunctions.route()
        .GET("/", request -> redirectToConsole())
        .GET("/console", request -> redirectToConsole())
        .build()
        .and(RouterFunctions.resources(ConsoleUiConfiguration::lookup, ConsoleUiConfiguration::cacheHeaders));
  }

  private static Mono<ServerResponse> redirectToConsole() {
    // A relative location keeps the host and port the browser used.
    return ServerResponse.status(302).location(URI.create(CONSOLE_PATH)).build();
  }

  private static Mono<Resource> lookup(ServerRequest request) {
    String path = request.path();
    if (!path.startsWith(CONSOLE_PATH)) {
      return Mono.empty();
    }
    String relative = path.substring(CONSOLE_PATH.length());
    if (relative.contains("..")) {
      return Mono.empty();
    }

    Resource file = new ClassPathResource(UI_LOCATION + relative);
    if (!relative.isEmpty() && file.isReadable()) {
      return Mono.just(file);
    }
    // A missing build file (e.g. from a previous version) is a real 404, not the app's HTML.
    if (HASHED_FILE.matcher(relative).matches()) {
      return Mono.empty();
    }
    // Everything else is a page of the app, like /console/aggregates/123: the app shows it.
    Resource index = new ClassPathResource(UI_LOCATION + "index.html");
    return index.isReadable() ? Mono.just(index) : Mono.empty();
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
