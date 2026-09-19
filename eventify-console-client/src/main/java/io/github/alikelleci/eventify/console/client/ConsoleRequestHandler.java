package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Requests;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.github.alikelleci.eventify.core.message.MessageIds;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/** Answers the console's requests with {@link ConsoleService}. Blocking: call it off the network threads. */
@Slf4j
class ConsoleRequestHandler {

  static final int DEFAULT_PAGE_SIZE = 50;
  static final int MAX_PAGE_SIZE = 500;

  private final ConsoleService service;
  /** Eventify's own mapper, for the events and commands: the console shows them as the application writes them. */
  private final ObjectMapper eventifyMapper;
  /** For the protocol's own messages. */
  private final ObjectMapper protocolMapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  ConsoleRequestHandler(ConsoleService service, ObjectMapper eventifyMapper) {
    this.service = service;
    this.eventifyMapper = eventifyMapper;
  }

  Reply handle(String routeName, byte[] data, CancelSignal cancel) {
    Route route;
    try {
      route = Route.valueOf(String.valueOf(routeName));
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
          String aggregateId = requireAggregateId(request.aggregateId());
          yield toReply(service.getEventDetail(aggregateId, requireEventOf(aggregateId, request.eventId())));
        }
        case EVENTS_OF_COMMAND -> {
          Requests.EventsOfCommand request = read(data, Requests.EventsOfCommand.class);
          String aggregateId = requireAggregateId(request.aggregateId());
          yield toReply(service.getEventsOfCommand(aggregateId, requireMessageOf(aggregateId, "commandId", request.commandId()), request.correlationId()));
        }
        case STATE -> {
          Requests.State request = read(data, Requests.State.class);
          String aggregateId = requireAggregateId(request.aggregateId());
          yield toReply(service.getState(aggregateId, request.eventId() == null ? null : requireEventOf(aggregateId, request.eventId())));
        }
        case COMMANDS -> {
          Requests.Commands request = read(data, Requests.Commands.class);
          yield toReply(service.getCommands(requireAggregateId(request.aggregateId()), clampLimit(request.limit(), MAX_PAGE_SIZE), cancel));
        }
        case RETRY_COMMAND -> toReply(service.retryCommand(data));
        case STATUS -> toReply(service.getStatus());
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
    return require("aggregateId", aggregateId);
  }

  /**
   * An event of this aggregate. Its id starts with the aggregate id: another aggregate's event would make a replay
   * apply every event stored between the two.
   */
  private static String requireEventOf(String aggregateId, String eventId) {
    return requireMessageOf(aggregateId, "eventId", eventId);
  }

  /** A message (event or command) of this aggregate: its id is the aggregate id, "@" and a ULID. */
  private static String requireMessageOf(String aggregateId, String name, String messageId) {
    if (!MessageIds.isKeyOf(aggregateId, require(name, messageId))) {
      throw new BadRequestException(name + " " + messageId + " is not of aggregate " + aggregateId);
    }
    return messageId;
  }

  private static String require(String name, String value) {
    if (value == null || value.isBlank()) {
      throw new BadRequestException(name + " is required");
    }
    return value;
  }

  /** The header as it is; the answer as JSON, only when there is one. */
  private Reply toReply(ConsoleViews.Result<?> result) throws IOException {
    if (!result.isOk() || result.value() == null) {
      return Reply.of(result.header());
    }
    return new Reply(result.header(), eventifyMapper.writeValueAsBytes(result.value()));
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
