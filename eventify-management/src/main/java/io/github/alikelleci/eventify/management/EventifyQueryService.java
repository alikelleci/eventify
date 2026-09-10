package io.github.alikelleci.eventify.management;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class EventifyQueryService {

  public static final int DEFAULT_PAGE_SIZE = 50;
  public static final int MAX_PAGE_SIZE = 500;

  private static final String EVENT_STORE = "event-store";
  private static final String SNAPSHOT_STORE = "snapshot-store";

  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);

  public record EventsPage(List<Event> events, String nextCursor) {}
  public record EventDetail(Event event, AggregateState state, AggregateState previousState) {}

  public sealed interface QueryResult<T> {
    record Ok<T>(T value) implements QueryResult<T> {}
    record NotFound<T>() implements QueryResult<T> {}
    record Unavailable<T>(String reason) implements QueryResult<T> {}
    record RemoteError<T>(int statusCode) implements QueryResult<T> {}
  }

  private final Eventify eventify;
  private final HostInfo thisHost;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  public EventifyQueryService(Eventify eventify) {
    this.eventify = eventify;
    this.objectMapper = eventify.getObjectMapper();
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(CONNECT_TIMEOUT)
        .build();

    String applicationServer = eventify.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, "");
    URI uri = applicationServer.isBlank() ? null : URI.create("http://" + applicationServer);
    this.thisHost = (uri != null && uri.getHost() != null && uri.getPort() != -1)
        ? new HostInfo(uri.getHost(), uri.getPort())
        : HostInfo.unavailable();

    if (thisHost.equals(HostInfo.unavailable())) {
      log.warn("'{}' is not configured, running in single-node mode. Multi-node routing is disabled.",
          StreamsConfig.APPLICATION_SERVER_CONFIG);
    }
  }

  public QueryResult<EventsPage> getEvents(String aggregateId, String cursor, int limit, boolean forwarded) {
    QueryResult<EventsPage> routing = checkRouting(aggregateId, forwarded,
        "/api/aggregates/" + URLEncoder.encode(aggregateId, java.nio.charset.StandardCharsets.UTF_8) + "/events" + buildEventsQuery(cursor, limit),
        new TypeReference<EventsPage>() {});
    if (routing != null) {
      return routing;
    }

    try {
      ReadOnlyKeyValueStore<String, Event> store = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));

      String from = aggregateId + "@";
      String to = cursor != null ? aggregateId + "@" + cursor : aggregateId + "@~";

      List<Event> events = new ArrayList<>();
      try (KeyValueIterator<String, Event> it = store.reverseRange(from, to)) {
        while (it.hasNext() && events.size() <= limit) {
          events.add(it.next().value);
        }
      }

      String nextCursor = null;
      if (events.size() > limit) {
        Event extra = events.remove(events.size() - 1);
        nextCursor = extra.getId().substring(aggregateId.length() + 1);
      }

      return new QueryResult.Ok<>(new EventsPage(events, nextCursor));
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return new QueryResult.Unavailable<>("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying events for aggregate {}", aggregateId, e);
      return new QueryResult.Unavailable<>("Unexpected error");
    }
  }

  public QueryResult<EventDetail> getEventDetail(String aggregateId, String eventId, boolean forwarded) {
    QueryResult<EventDetail> routing = checkRouting(aggregateId, forwarded,
        "/api/aggregates/" + URLEncoder.encode(aggregateId, java.nio.charset.StandardCharsets.UTF_8) + "/events/" + URLEncoder.encode(eventId, java.nio.charset.StandardCharsets.UTF_8),
        new TypeReference<EventDetail>() {});
    if (routing != null) {
      return routing;
    }

    try {
      ReadOnlyKeyValueStore<String, AggregateState> snapshotStore = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(SNAPSHOT_STORE, QueryableStoreTypes.keyValueStore()));
      ReadOnlyKeyValueStore<String, Event> eventStore = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));

      Event targetEvent = eventStore.get(eventId);
      if (targetEvent == null) {
        return new QueryResult.NotFound<>();
      }

      String from = aggregateId + "@";
      String to = eventId;

      AggregateState state = Optional.ofNullable(snapshotStore.get(aggregateId))
          .filter(snap -> snap.getEventId().compareTo(to) <= 0)
          .orElse(null);

      if (state != null) {
        from = state.getEventId() + "\0";
      }

      long version = state != null ? state.getVersion() : 0;
      AggregateState previousState = state;

      try (KeyValueIterator<String, Event> it = eventStore.range(from, to)) {
        while (it.hasNext()) {
          Event event = it.next().value;
          boolean isTarget = event.getId().equals(eventId);
          if (isTarget) previousState = state;
          EventSourcingHandler handler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
          if (handler != null) {
            state = handler.apply(state, event);
            version++;
          }
        }
      }

      if (state == null) {
        return new QueryResult.NotFound<>();
      }

      AggregateState currentState = AggregateState.builder()
          .timestamp(state.getTimestamp())
          .payload(state.getPayload())
          .metadata(state.getMetadata())
          .eventId(state.getEventId())
          .version(version)
          .build();

      return new QueryResult.Ok<>(new EventDetail(targetEvent, currentState, previousState));
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return new QueryResult.Unavailable<>("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying event detail for aggregate {}", aggregateId, e);
      return new QueryResult.Unavailable<>("Unexpected error");
    }
  }

  public QueryResult<AggregateState> getState(String aggregateId, String eventId, boolean forwarded) {
    QueryResult<AggregateState> routing = checkRouting(aggregateId, forwarded,
        "/api/aggregates/" + URLEncoder.encode(aggregateId, java.nio.charset.StandardCharsets.UTF_8) + "/state" + buildStateQuery(eventId),
        new TypeReference<AggregateState>() {});
    if (routing != null) {
      return routing;
    }

    try {
      ReadOnlyKeyValueStore<String, AggregateState> snapshotStore = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(SNAPSHOT_STORE, QueryableStoreTypes.keyValueStore()));
      ReadOnlyKeyValueStore<String, Event> eventStore = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));

      String from = aggregateId + "@";
      String to = eventId != null ? eventId : aggregateId + "@~";

      AggregateState state = Optional.ofNullable(snapshotStore.get(aggregateId))
          .filter(snap -> eventId == null || snap.getEventId().compareTo(to) <= 0)
          .orElse(null);

      if (state != null) {
        from = state.getEventId() + "\0";
      }

      long version = state != null ? state.getVersion() : 0;

      try (KeyValueIterator<String, Event> it = eventStore.range(from, to)) {
        while (it.hasNext()) {
          Event event = it.next().value;
          EventSourcingHandler handler = eventify.getEventSourcingHandlers().get(event.getPayload().getClass());
          if (handler != null) {
            state = handler.apply(state, event);
            version++;
          }
        }
      }

      if (state == null) {
        if (!isLocallyAuthoritative(aggregateId)) {
          log.debug("Ownership/availability changed while querying aggregate {}; returning 503", aggregateId);
          return new QueryResult.Unavailable<>("Ownership changed during query");
        }
        return new QueryResult.NotFound<>();
      }

      state = AggregateState.builder()
          .timestamp(state.getTimestamp())
          .payload(state.getPayload())
          .metadata(state.getMetadata())
          .eventId(state.getEventId())
          .version(version)
          .build();

      return new QueryResult.Ok<>(state);
    } catch (InvalidStateStoreException e) {
      log.warn("Event store not ready for aggregate {}", aggregateId, e);
      return new QueryResult.Unavailable<>("Event store not ready");
    } catch (Exception e) {
      log.error("Unexpected error querying state for aggregate {}", aggregateId, e);
      return new QueryResult.Unavailable<>("Unexpected error");
    }
  }

  private <T> QueryResult<T> checkRouting(String aggregateId, boolean forwarded, String path, TypeReference<T> responseType) {
    KafkaStreams streams = eventify.getKafkaStreams();

    if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
      log.warn("Kafka Streams is not running");
      return new QueryResult.Unavailable<>("Kafka Streams is not running");
    }

    if (thisHost.equals(HostInfo.unavailable())) {
      return null;
    }

    KeyQueryMetadata metadata = streams.queryMetadataForKey(EVENT_STORE, aggregateId, Serdes.String().serializer());
    if (metadata == null || metadata.activeHost().equals(HostInfo.unavailable())) {
      log.warn("Metadata unavailable for aggregate {}", aggregateId);
      return new QueryResult.Unavailable<>("Metadata unavailable");
    }

    HostInfo activeHost = metadata.activeHost();
    if (activeHost.equals(thisHost)) {
      return null;
    }

    if (forwarded) {
      log.warn("Aggregate {} not owned by this node after forwarding, possible rebalance", aggregateId);
      return new QueryResult.Unavailable<>("Not owned by this node after forwarding");
    }

    return forward(aggregateId, activeHost, path, responseType);
  }

  private <T> QueryResult<T> forward(String aggregateId, HostInfo target, String path, TypeReference<T> responseType) {
    try {
      String separator = path.contains("?") ? "&" : "?";
      String url = "http://" + target.host() + ":" + target.port() + path + separator + "forwarded=true";
      log.debug("Forwarding request for aggregate {} to {}", aggregateId, target);

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(READ_TIMEOUT)
          .GET()
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      int status = response.statusCode();
      if (status == 200) {
        return new QueryResult.Ok<>(objectMapper.readValue(response.body(), responseType));
      } else if (status == 404) {
        return new QueryResult.NotFound<>();
      } else {
        log.warn("Remote node {} returned {} for aggregate {}", target, status, aggregateId);
        return new QueryResult.RemoteError<>(status);
      }
    } catch (IOException | InterruptedException e) {
      log.warn("Failed to forward request for aggregate {} to {}", aggregateId, target, e);
      return new QueryResult.Unavailable<>("Failed to reach remote node");
    }
  }

  private boolean isLocallyAuthoritative(String aggregateId) {
    KafkaStreams streams = eventify.getKafkaStreams();
    if (streams.state() != KafkaStreams.State.RUNNING) {
      return false;
    }
    if (thisHost.equals(HostInfo.unavailable())) {
      return true;
    }
    KeyQueryMetadata metadata = streams.queryMetadataForKey(EVENT_STORE, aggregateId, Serdes.String().serializer());
    return metadata != null && thisHost.equals(metadata.activeHost());
  }

  private String buildEventsQuery(String cursor, int limit) {
    StringBuilder sb = new StringBuilder("?limit=").append(limit);
    if (cursor != null) sb.append("&cursor=").append(URLEncoder.encode(cursor, java.nio.charset.StandardCharsets.UTF_8));
    return sb.toString();
  }

  private String buildStateQuery(String eventId) {
    return eventId != null ? "?eventId=" + URLEncoder.encode(eventId, java.nio.charset.StandardCharsets.UTF_8) : "";
  }
}
