package io.github.alikelleci.eventify.console.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * The Eventify Console. Applications connect to it over RSocket; the browser talks to it over HTTP, and it passes
 * each request on to an instance of the chosen application.
 */
@SpringBootApplication
@EnableConfigurationProperties(ConsoleProperties.class)
public class EventifyConsoleApplication {

  public static void main(String[] args) {
    SpringApplication.run(EventifyConsoleApplication.class, args);
  }
}
