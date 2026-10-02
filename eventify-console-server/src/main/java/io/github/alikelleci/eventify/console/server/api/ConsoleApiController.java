package io.github.alikelleci.eventify.console.server.api;

import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.Requests;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.github.alikelleci.eventify.console.server.node.NodeGateway;
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

/** Queries about the aggregates of an application, and commands to retry: answered by an instance of the application. */
@RestController
@RequestMapping("/api/apps")
@RequiredArgsConstructor
public class ConsoleApiController {

  private final NodeGateway gateway;
  private final JsonMapper jsonMapper;

  @GetMapping("/{app}/aggregates/{aggregateType}/{aggregateId}/events")
  public Mono<ResponseEntity<byte[]>> events(@PathVariable String app, @PathVariable String aggregateType, @PathVariable String aggregateId,
                                             @RequestParam(required = false) Long cursor,
                                             @RequestParam(required = false) Integer limit) {
    return toOwner(app, Route.EVENTS, aggregateId, new Requests.Events(aggregateType, aggregateId, cursor, limit));
  }

  @GetMapping("/{app}/aggregates/{aggregateType}/{aggregateId}/events/{sequence}")
  public Mono<ResponseEntity<byte[]>> eventDetail(@PathVariable String app, @PathVariable String aggregateType, @PathVariable String aggregateId,
                                                  @PathVariable long sequence) {
    return toOwner(app, Route.EVENT_DETAIL, aggregateId, new Requests.EventDetail(aggregateType, aggregateId, sequence));
  }

  /** The events the command produced: the ones that name it as their cause. */
  @GetMapping("/{app}/aggregates/{aggregateType}/{aggregateId}/commands/{commandId}/events")
  public Mono<ResponseEntity<byte[]>> eventsOfCommand(@PathVariable String app, @PathVariable String aggregateType, @PathVariable String aggregateId,
                                                      @PathVariable String commandId) {
    return toOwner(app, Route.EVENTS_OF_COMMAND, aggregateId, new Requests.EventsOfCommand(aggregateType, aggregateId, commandId));
  }

  @GetMapping("/{app}/aggregates/{aggregateType}/{aggregateId}/state")
  public Mono<ResponseEntity<byte[]>> state(@PathVariable String app, @PathVariable String aggregateType, @PathVariable String aggregateId,
                                            @RequestParam(required = false) Long sequence) {
    return toOwner(app, Route.STATE, aggregateId, new Requests.State(aggregateType, aggregateId, sequence));
  }

  @GetMapping("/{app}/aggregates/{aggregateType}/{aggregateId}/commands")
  public Mono<ResponseEntity<byte[]>> commands(@PathVariable String app, @PathVariable String aggregateType, @PathVariable String aggregateId,
                                               @RequestParam(required = false) Integer limit) {
    return gateway.sendToAny(app, Route.COMMANDS, jsonMapper.writeValueAsBytes(new Requests.Commands(aggregateType, aggregateId, limit)))
        .map(ConsoleApiController::toResponse);
  }

  /**
   * The body is the command as the UI received it; the console passes it on without reading it. Only as JSON: a web
   * page elsewhere can't send that to the console without the browser asking the console first, which it refuses.
   */
  @PostMapping(value = "/{app}/commands/retry", consumes = MediaType.APPLICATION_JSON_VALUE)
  public Mono<ResponseEntity<byte[]>> retryCommand(@PathVariable String app, @RequestBody byte[] command) {
    return gateway.sendToAny(app, Route.RETRY_COMMAND, command).map(ConsoleApiController::toResponse);
  }

  private Mono<ResponseEntity<byte[]>> toOwner(String app, Route route, String aggregateId, Object request) {
    return gateway.sendToOwner(app, route, aggregateId, jsonMapper.writeValueAsBytes(request))
        .map(ConsoleApiController::toResponse);
  }

  static ResponseEntity<byte[]> toResponse(Reply reply) {
    return switch (reply.header().status()) {
      case OK -> reply.body().length == 0
          ? ResponseEntity.ok().build()
          : ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(reply.body());
      case NOT_FOUND -> text(HttpStatus.NOT_FOUND, "Not Found");
      case BAD_REQUEST -> text(HttpStatus.BAD_REQUEST, reply.header().reason());
      case UNREADABLE -> text(HttpStatus.UNPROCESSABLE_CONTENT, reply.header().reason());
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
