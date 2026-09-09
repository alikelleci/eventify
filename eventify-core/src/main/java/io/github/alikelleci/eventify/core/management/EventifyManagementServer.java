package io.github.alikelleci.eventify.core.management;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.management.EventifyQueryService.QueryResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

@Slf4j
public class EventifyManagementServer {

  private static final String BASE_PATH = "/_eventify/";

  private final EventifyQueryService queryService;
  private final ObjectMapper objectMapper;
  private final int port;
  private HttpServer server;

  public EventifyManagementServer(EventifyQueryService queryService, ObjectMapper objectMapper, int port) {
    this.queryService = queryService;
    this.objectMapper = objectMapper;
    this.port = port;
  }

  public void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(BASE_PATH, this::handle);
    server.setExecutor(Executors.newCachedThreadPool());
    server.start();
    log.info("Eventify management server started on port {}", port);
  }

  public void stop() {
    if (server != null) {
      server.stop(0);
      log.info("Eventify management server stopped.");
    }
  }

  private void handle(HttpExchange exchange) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      sendResponse(exchange, 405, "Method Not Allowed");
      return;
    }

    try {
      URI uri = exchange.getRequestURI();
      String path = uri.getPath();
      Map<String, String> queryParams = parseQueryParams(uri.getQuery());

      // path: /_eventify/{aggregateId}/events or /_eventify/{aggregateId}/state
      String[] segments = path.split("/");
      // segments: ["", "_eventify", "{aggregateId}", "events|state"]
      if (segments.length != 4) {
        sendResponse(exchange, 404, "Not Found");
        return;
      }

      String aggregateId = URLDecoder.decode(segments[2], StandardCharsets.UTF_8);
      String endpoint = segments[3];
      boolean forwarded = Boolean.parseBoolean(queryParams.get("forwarded"));

      switch (endpoint) {
        case "events" -> handleGetEvents(exchange, aggregateId, queryParams, forwarded);
        case "state" -> handleGetState(exchange, aggregateId, queryParams, forwarded);
        default -> sendResponse(exchange, 404, "Not Found");
      }
    } catch (Exception e) {
      log.error("Unexpected error handling management request", e);
      sendResponse(exchange, 500, "Internal Server Error");
    }
  }

  private void handleGetEvents(HttpExchange exchange, String aggregateId,
                               Map<String, String> queryParams, boolean forwarded) throws IOException {
    String cursor = queryParams.get("cursor");
    int limit = clampLimit(parseIntOrDefault(queryParams.get("limit"), EventifyQueryService.DEFAULT_PAGE_SIZE));

    QueryResult<EventifyQueryService.EventsPage> result = queryService.getEvents(aggregateId, cursor, limit, forwarded);
    sendQueryResult(exchange, result);
  }

  private void handleGetState(HttpExchange exchange, String aggregateId,
                              Map<String, String> queryParams, boolean forwarded) throws IOException {
    String atParam = queryParams.get("at");
    Instant at = atParam != null ? Instant.parse(atParam) : null;

    QueryResult<?> result = queryService.getState(aggregateId, at, forwarded);
    sendQueryResult(exchange, result);
  }

  private void sendQueryResult(HttpExchange exchange, QueryResult<?> result) throws IOException {
    if (result instanceof QueryResult.Ok<?> ok) {
      sendJson(exchange, 200, ok.value());
    } else if (result instanceof QueryResult.NotFound<?>) {
      sendResponse(exchange, 404, "Not Found");
    } else if (result instanceof QueryResult.RemoteError<?> r) {
      sendResponse(exchange, r.statusCode(), "Remote error");
    } else if (result instanceof QueryResult.Unavailable<?> u) {
      sendResponse(exchange, 503, u.reason());
    }
  }

  private void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
    byte[] bytes = objectMapper.writeValueAsBytes(body);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "text/plain");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private Map<String, String> parseQueryParams(String query) {
    Map<String, String> params = new HashMap<>();
    if (query == null || query.isBlank()) return params;
    Arrays.stream(query.split("&"))
        .map(pair -> pair.split("=", 2))
        .filter(pair -> pair.length == 2)
        .forEach(pair -> params.put(
            URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
            URLDecoder.decode(pair[1], StandardCharsets.UTF_8)));
    return params;
  }

  private int parseIntOrDefault(String value, int defaultValue) {
    if (value == null) return defaultValue;
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private int clampLimit(int limit) {
    return Math.max(1, Math.min(limit, EventifyQueryService.MAX_PAGE_SIZE));
  }
}
