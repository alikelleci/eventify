package io.github.alikelleci.eventify.console.server.api;

import io.github.alikelleci.eventify.console.protocol.Requests;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.github.alikelleci.eventify.console.server.node.NodeGateway;
import io.github.alikelleci.eventify.console.server.node.Reply;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

/** Queries about one aggregate: every one of them is answered by an instance of the application it belongs to. */
@RestController
@RequestMapping("/api/apps")
@RequiredArgsConstructor
public class ConsoleApiController {

  private final NodeGateway gateway;
  private final JsonMapper jsonMapper;

  @GetMapping("/{app}/aggregates/{aggregateId}/events")
  public Mono<ResponseEntity<byte[]>> events(@PathVariable String app, @PathVariable String aggregateId,
                                             @RequestParam(required = false) String cursor,
                                             @RequestParam(required = false) Integer limit) {
    return send(app, Route.EVENTS, aggregateId, new Requests.Events(aggregateId, cursor, limit));
  }

  @GetMapping("/{app}/aggregates/{aggregateId}/events/{eventId}")
  public Mono<ResponseEntity<byte[]>> eventDetail(@PathVariable String app, @PathVariable String aggregateId,
                                                  @PathVariable String eventId) {
    return send(app, Route.EVENT_DETAIL, aggregateId, new Requests.EventDetail(aggregateId, eventId));
  }

  @GetMapping("/{app}/aggregates/{aggregateId}/events/by-correlation/{correlationId}")
  public Mono<ResponseEntity<byte[]>> eventsByCorrelation(@PathVariable String app, @PathVariable String aggregateId,
                                                          @PathVariable String correlationId) {
    return send(app, Route.EVENTS_BY_CORRELATION, aggregateId, new Requests.EventsByCorrelation(aggregateId, correlationId));
  }

  @GetMapping("/{app}/aggregates/{aggregateId}/state")
  public Mono<ResponseEntity<byte[]>> state(@PathVariable String app, @PathVariable String aggregateId,
                                            @RequestParam(required = false) String eventId) {
    return send(app, Route.STATE, aggregateId, new Requests.State(aggregateId, eventId));
  }

  @GetMapping("/{app}/aggregates/{aggregateId}/commands")
  public Mono<ResponseEntity<byte[]>> commands(@PathVariable String app, @PathVariable String aggregateId,
                                               @RequestParam(required = false) Integer limit) {
    return send(app, Route.COMMANDS, aggregateId, new Requests.Commands(aggregateId, limit));
  }

  /** The body is the command as the UI received it; the console passes it on without reading it. */
  @PostMapping("/{app}/aggregates/{aggregateId}/commands/{commandId}/retry")
  public Mono<ResponseEntity<byte[]>> retryCommand(@PathVariable String app, @PathVariable String aggregateId,
                                                   @RequestBody byte[] command) {
    return gateway.send(app, Route.RETRY_COMMAND, aggregateId, command).map(ConsoleApiController::toResponse);
  }

  private Mono<ResponseEntity<byte[]>> send(String app, Route route, String aggregateId, Object request) {
    return gateway.send(app, route, aggregateId, jsonMapper.writeValueAsBytes(request))
        .map(ConsoleApiController::toResponse);
  }

  static ResponseEntity<byte[]> toResponse(Reply reply) {
    return switch (reply.header().status()) {
      case OK -> reply.body().length == 0
          ? ResponseEntity.ok().build()
          : ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(reply.body());
      case NOT_FOUND -> text(HttpStatus.NOT_FOUND, "Not Found");
      case BAD_REQUEST -> text(HttpStatus.BAD_REQUEST, reply.header().reason());
      // NOT_OWNER never reaches here: the gateway follows it, or turns it into UNAVAILABLE.
      case UNAVAILABLE, NOT_OWNER -> text(HttpStatus.SERVICE_UNAVAILABLE, reply.header().reason());
    };
  }

  private static ResponseEntity<byte[]> text(HttpStatus status, String message) {
    return ResponseEntity.status(status)
        .contentType(MediaType.TEXT_PLAIN)
        .body(String.valueOf(message).getBytes(StandardCharsets.UTF_8));
  }
}
