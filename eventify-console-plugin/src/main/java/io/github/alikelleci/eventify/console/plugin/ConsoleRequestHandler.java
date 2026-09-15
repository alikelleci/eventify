package io.github.alikelleci.eventify.console.plugin;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Requests;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/** Answers the console's requests with {@link EventifyService}. Blocking: call it off the network threads. */
@Slf4j
public class ConsoleRequestHandler {

  public static final int DEFAULT_PAGE_SIZE = 50;
  public static final int MAX_PAGE_SIZE = 500;

  /** The reply to a request: the outcome, and the JSON body when it's {@link ReplyHeader.Status#OK}. */
  public record Reply(ReplyHeader header, byte[] body) {
    static Reply of(ReplyHeader header) {
      return new Reply(header, new byte[0]);
    }
  }

  private final EventifyService service;
  /** Eventify's own mapper, for the events and commands: the console shows them as the application writes them. */
  private final ObjectMapper eventifyMapper;
  /** For the protocol's own messages. */
  private final ObjectMapper protocolMapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public ConsoleRequestHandler(EventifyService service, ObjectMapper eventifyMapper) {
    this.service = service;
    this.eventifyMapper = eventifyMapper;
  }

  public Reply handle(String routeName, byte[] data) {
    Route route;
    try {
      route = Route.valueOf(routeName);
    } catch (IllegalArgumentException e) {
      return Reply.of(ReplyHeader.badRequest("Unknown route: " + routeName));
    }

    try {
      return switch (route) {
        case EVENTS -> {
          Requests.Events request = read(data, Requests.Events.class);
          yield toReply(service.getEvents(requireAggregateId(request.aggregateId()), request.cursor(), clampLimit(request.limit(), DEFAULT_PAGE_SIZE)));
        }
        case EVENT_DETAIL -> {
          Requests.EventDetail request = read(data, Requests.EventDetail.class);
          yield toReply(service.getEventDetail(requireAggregateId(request.aggregateId()), request.eventId()));
        }
        case EVENTS_BY_CORRELATION -> {
          Requests.EventsByCorrelation request = read(data, Requests.EventsByCorrelation.class);
          yield toReply(service.getEventsByCorrelation(requireAggregateId(request.aggregateId()), request.correlationId()));
        }
        case STATE -> {
          Requests.State request = read(data, Requests.State.class);
          yield toReply(service.getState(requireAggregateId(request.aggregateId()), request.eventId()));
        }
        case COMMANDS -> {
          Requests.Commands request = read(data, Requests.Commands.class);
          yield toReply(service.getCommands(requireAggregateId(request.aggregateId()), clampLimit(request.limit(), MAX_PAGE_SIZE)));
        }
        case RETRY_COMMAND -> toReply(service.retryCommand(eventifyMapper.readValue(data, Command.class)));
      };
    } catch (BadRequestException e) {
      return Reply.of(ReplyHeader.badRequest(e.getMessage()));
    } catch (IOException e) {
      log.warn("Failed to read console request for route {}", route, e);
      return Reply.of(ReplyHeader.badRequest("Invalid request data"));
    } catch (Exception e) {
      log.error("Unexpected error handling console request for route {}", route, e);
      return Reply.of(ReplyHeader.unavailable("Unexpected error"));
    }
  }

  private <T> T read(byte[] data, Class<T> type) throws IOException {
    return protocolMapper.readValue(data, type);
  }

  /** Every query is about one aggregate; without its id there is nothing to look up. */
  private static String requireAggregateId(String aggregateId) {
    if (aggregateId == null || aggregateId.isBlank()) {
      throw new BadRequestException("aggregateId is required");
    }
    return aggregateId;
  }

  private Reply toReply(EventifyService.ApiResult<?> result) throws IOException {
    if (result instanceof EventifyService.ApiResult.Ok<?> ok) {
      byte[] body = ok.value() == null ? new byte[0] : eventifyMapper.writeValueAsBytes(ok.value());
      return new Reply(ReplyHeader.ok(), body);
    } else if (result instanceof EventifyService.ApiResult.NotFound<?>) {
      return Reply.of(ReplyHeader.notFound());
    } else if (result instanceof EventifyService.ApiResult.NotOwner<?> notOwner) {
      return Reply.of(ReplyHeader.notOwner(notOwner.owner()));
    } else if (result instanceof EventifyService.ApiResult.Unavailable<?> unavailable) {
      return Reply.of(ReplyHeader.unavailable(unavailable.reason()));
    }
    throw new IllegalStateException("Unknown result: " + result);
  }

  private static int clampLimit(Integer limit, int defaultValue) {
    return Math.max(1, Math.min(limit != null ? limit : defaultValue, MAX_PAGE_SIZE));
  }

  private static class BadRequestException extends RuntimeException {
    BadRequestException(String message) {
      super(message);
    }
  }
}
