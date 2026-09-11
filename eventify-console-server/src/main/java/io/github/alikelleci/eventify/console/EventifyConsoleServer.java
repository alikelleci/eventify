package io.github.alikelleci.eventify.console;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.EventifyQueryService.QueryResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

@Slf4j
public class EventifyConsoleServer {

  public static final int DEFAULT_PAGE_SIZE = 50;
  public static final int MAX_PAGE_SIZE = 500;

  private static final String API_PATH = "/api/";
  private static final String CONSOLE_PATH = "/console/";
  private static final String CONSOLE_RESOURCES = "META-INF/resources/console/";

  private final EventifyQueryService queryService;
  private final ObjectMapper objectMapper;
  private final int port;
  private HttpServer server;

  public EventifyConsoleServer(EventifyQueryService queryService, ObjectMapper objectMapper, int port) {
    this.queryService = queryService;
    this.objectMapper = objectMapper;
    this.port = port;
  }

  public void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(API_PATH, this::handle);
    server.createContext(CONSOLE_PATH, this::handleUi);
    server.createContext("/console", exchange -> {
      exchange.getResponseHeaders().set("Location", "/console/");
      exchange.sendResponseHeaders(301, -1);
      exchange.close();
    });
    server.setExecutor(Executors.newCachedThreadPool());
    server.start();
    log.info("Eventify console server started on port {}", port);
  }

  public void stop() {
    if (server != null) {
      server.stop(0);
      log.info("Eventify console server stopped.");
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

      String[] segments = path.split("/");

      boolean forwarded = Boolean.parseBoolean(queryParams.get("forwarded"));

      // /api/aggregates/{id}/events/{eventId}
      if (segments.length == 6 && "aggregates".equals(segments[2]) && "events".equals(segments[4])) {
        String aggregateId = URLDecoder.decode(segments[3], StandardCharsets.UTF_8);
        String eventId = URLDecoder.decode(segments[5], StandardCharsets.UTF_8);
        QueryResult<EventifyQueryService.EventDetail> result = queryService.getEventDetail(aggregateId, eventId, forwarded);
        sendQueryResult(exchange, result);
        return;
      }

      if (segments.length != 5 || !"aggregates".equals(segments[2])) {
        sendResponse(exchange, 404, "Not Found");
        return;
      }

      String aggregateId = URLDecoder.decode(segments[3], StandardCharsets.UTF_8);
      String endpoint = segments[4];

      switch (endpoint) {
        case "events" -> handleGetEvents(exchange, aggregateId, queryParams, forwarded);
        case "state" -> handleGetState(exchange, aggregateId, queryParams, forwarded);
        default -> sendResponse(exchange, 404, "Not Found");
      }
    } catch (Exception e) {
      log.error("Unexpected error handling console request", e);
      sendResponse(exchange, 500, "Internal Server Error");
    }
  }

  private void handleGetEvents(HttpExchange exchange, String aggregateId,
                               Map<String, String> queryParams, boolean forwarded) throws IOException {
    String cursor = queryParams.get("cursor");
    int limit = clampLimit(parseIntOrDefault(queryParams.get("limit"), DEFAULT_PAGE_SIZE));

    QueryResult<EventifyQueryService.EventsPage> result = queryService.getEvents(aggregateId, cursor, limit, forwarded);
    sendQueryResult(exchange, result);
  }

  private void handleGetState(HttpExchange exchange, String aggregateId,
                              Map<String, String> queryParams, boolean forwarded) throws IOException {
    String eventId = queryParams.get("eventId");
    QueryResult<?> result = queryService.getState(aggregateId, eventId, forwarded);
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
    return Math.max(1, Math.min(limit, MAX_PAGE_SIZE));
  }

  private void handleUi(HttpExchange exchange) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      sendResponse(exchange, 405, "Method Not Allowed");
      return;
    }

    String path = exchange.getRequestURI().getPath();
    String resource = path.substring(CONSOLE_PATH.length());
    if (resource.isEmpty() || resource.equals("/")) {
      resource = "/index.html";
    }

    String classpathPath = CONSOLE_RESOURCES + resource.replaceFirst("^/", "");
    try (java.io.InputStream is = getClass().getClassLoader().getResourceAsStream(classpathPath)) {
      if (is == null) {
        try (java.io.InputStream fallback = getClass().getClassLoader().getResourceAsStream(CONSOLE_RESOURCES + "index.html")) {
          if (fallback == null) {
            sendResponse(exchange, 404, "Eventify Console not available");
            return;
          }
          serveStream(exchange, fallback, "text/html");
        }
        return;
      }
      serveStream(exchange, is, mimeType(resource));
    }
  }

  private void serveStream(HttpExchange exchange, java.io.InputStream is, String contentType) throws IOException {
    byte[] bytes = is.readAllBytes();
    exchange.getResponseHeaders().set("Content-Type", contentType);
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private String mimeType(String path) {
    if (path.endsWith(".html")) return "text/html";
    if (path.endsWith(".js"))   return "application/javascript";
    if (path.endsWith(".css"))  return "text/css";
    if (path.endsWith(".ico"))  return "image/x-icon";
    if (path.endsWith(".png"))  return "image/png";
    if (path.endsWith(".svg"))  return "image/svg+xml";
    if (path.endsWith(".woff2")) return "font/woff2";
    if (path.endsWith(".woff")) return "font/woff";
    return "application/octet-stream";
  }
}
