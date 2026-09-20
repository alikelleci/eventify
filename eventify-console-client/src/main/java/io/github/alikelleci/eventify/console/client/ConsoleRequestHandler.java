package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Requests;
import io.github.alikelleci.eventify.console.protocol.Route;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Set;

/** Answers the console's requests with {@link ConsoleService}. Blocking: call it off the network threads. */
@Slf4j
class ConsoleRequestHandler {

  static final int DEFAULT_PAGE_SIZE = 50;
  static final int MAX_PAGE_SIZE = 500;

  private final ConsoleService service;
  /** The aggregates this instance handles, fixed when it starts: a request for another one is answered as a bad one. */
  private final Set<String> aggregateTypes;
  /** Eventify's own mapper, for the events and commands: the console shows them as the application writes them. */
  private final ObjectMapper eventifyMapper;
  /** For the protocol's own messages. */
  private final ObjectMapper protocolMapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  ConsoleRequestHandler(ConsoleService service, ObjectMapper eventifyMapper, Set<String> aggregateTypes) {
    this.service = service;
    this.eventifyMapper = eventifyMapper;
    this.aggregateTypes = Set.copyOf(aggregateTypes);
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
          yield toReply(service.getEvents(new Requests.Events(
              requireAggregateType(request.aggregateType()),
              requireAggregateId(request.aggregateId()),
              request.cursor() == null ? null : requireSequence("cursor", request.cursor()),
              clampLimit(request.limit(), DEFAULT_PAGE_SIZE))));
        }
        case EVENT_DETAIL -> {
          Requests.EventDetail request = read(data, Requests.EventDetail.class);
          yield toReply(service.getEventDetail(new Requests.EventDetail(
              requireAggregateType(request.aggregateType()),
              requireAggregateId(request.aggregateId()),
              requireSequence("sequence", request.sequence()))));
        }
        case EVENTS_OF_COMMAND -> {
          Requests.EventsOfCommand request = read(data, Requests.EventsOfCommand.class);
          yield toReply(service.getEventsOfCommand(new Requests.EventsOfCommand(
              requireAggregateType(request.aggregateType()),
              requireAggregateId(request.aggregateId()),
              require("commandId", request.commandId()))));
        }
        case STATE -> {
          Requests.State request = read(data, Requests.State.class);
          yield toReply(service.getState(new Requests.State(
              requireAggregateType(request.aggregateType()),
              requireAggregateId(request.aggregateId()),
              request.sequence() == null ? null : requireSequence("sequence", request.sequence()))));
        }
        case COMMANDS -> {
          Requests.Commands request = read(data, Requests.Commands.class);
          yield toReply(service.getCommands(new Requests.Commands(
              requireAggregateType(request.aggregateType()),
              requireAggregateId(request.aggregateId()),
              clampLimit(request.limit(), MAX_PAGE_SIZE)), cancel));
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
   * An aggregate is addressed by its type and its identifier: one application can hold several aggregates. A name this
   * instance doesn't handle is refused instead of answered with nothing: an empty history would read as an aggregate
   * that has none, while the question itself is about an aggregate that isn't here.
   */
  private String requireAggregateType(String aggregateType) {
    String name = require("aggregateType", aggregateType);
    if (!aggregateTypes.contains(name)) {
      throw new BadRequestException("This application has no aggregate named '" + name + "'");
    }
    return name;
  }

  /** An event's sequence: 1 for an aggregate's first event, so never below 1. */
  private static long requireSequence(String name, Long sequence) {
    if (sequence == null || sequence < 1) {
      throw new BadRequestException(name + " must be a sequence of 1 or more");
    }
    return sequence;
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
