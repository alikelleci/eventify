package io.github.alikelleci.eventify.spring.starter;

import com.github.f4b6a3.ulid.UlidCreator;
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
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

@Slf4j
@RestController
@RequestMapping("/_eventify")
public class EventStoreController {

  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);

  private static final String EVENT_STORE = "event-store";
  private static final String SNAPSHOT_STORE = "snapshot-store";

  private final Eventify eventify;
  private final HostInfo thisHost;
  private final RestClient restClient;

  public EventStoreController(Eventify eventify) {
    this.eventify = eventify;

    SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
    factory.setConnectTimeout(CONNECT_TIMEOUT);
    factory.setReadTimeout(READ_TIMEOUT);
    this.restClient = RestClient.builder().requestFactory(factory).build();

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

  @GetMapping("/{aggregateId}/events")
  public ResponseEntity<List<Event>> getEvents(@PathVariable("aggregateId") String aggregateId,
                                               @RequestParam(name = "forwarded", defaultValue = "false") boolean forwarded,
                                               @RequestHeader(value = "Authorization", required = false) String authorization) {
    ResponseEntity<?> routingResult = checkRouting(aggregateId, forwarded, "/_eventify/{aggregateId}/events", authorization);
    if (routingResult != null) {
      if (routingResult.getStatusCode().is2xxSuccessful()) {
        @SuppressWarnings("unchecked")
        List<Event> body = (List<Event>) routingResult.getBody();
        return ResponseEntity.ok(body);
      }
      return ResponseEntity.status(routingResult.getStatusCode()).build();
    }

    try {
      ReadOnlyKeyValueStore<String, Event> store = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));

      List<Event> events = new ArrayList<>();
      String from = aggregateId + "@";
      String to = aggregateId + "@~";
      try (KeyValueIterator<String, Event> it = store.range(from, to)) {
        it.forEachRemaining(kv -> events.add(kv.value));
      }
      return ResponseEntity.ok(events);
    } catch (Exception e) {
      log.debug("Event store temporarily unavailable for aggregate {}", aggregateId, e);
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }
  }

  @GetMapping("/{aggregateId}/state")
  public ResponseEntity<Object> getState(@PathVariable("aggregateId") String aggregateId,
                                         @RequestParam(name = "at", required = false) Instant at,
                                         @RequestParam(name = "forwarded", defaultValue = "false") boolean forwarded,
                                         @RequestHeader(value = "Authorization", required = false) String authorization) {
    ResponseEntity<?> routingResult = checkRouting(aggregateId, forwarded, "/_eventify/{aggregateId}/state", authorization);
    if (routingResult != null) {
      if (routingResult.getStatusCode().is2xxSuccessful()) {
        return ResponseEntity.ok(routingResult.getBody());
      }
      return ResponseEntity.status(routingResult.getStatusCode()).build();
    }

    try {
      ReadOnlyKeyValueStore<String, AggregateState> snapshotStore = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(SNAPSHOT_STORE, QueryableStoreTypes.keyValueStore()));
      ReadOnlyKeyValueStore<String, Event> eventStore = eventify.getKafkaStreams()
          .store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));

      String from = aggregateId + "@";
      String to = at != null
          ? aggregateId + "@" + UlidCreator.getMonotonicUlid(at.toEpochMilli())
          : aggregateId + "@~";

      // Start from snapshot if available and not doing a point-in-time query before it
      AggregateState state = Optional.ofNullable(snapshotStore.get(aggregateId))
          .filter(snap -> at == null || snap.getEventId().compareTo(to) < 0)
          .orElse(null);

      if (state != null) {
        from = state.getEventId() + "\0"; // resume after snapshot event, same as CommandProcessor
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
        return ResponseEntity.status(NOT_FOUND).build();
      }

      state = AggregateState.builder()
          .timestamp(state.getTimestamp())
          .payload(state.getPayload())
          .metadata(state.getMetadata())
          .eventId(state.getEventId())
          .version(version)
          .build();

      return ResponseEntity.ok(state);
    } catch (Exception e) {
      log.debug("Event store temporarily unavailable for aggregate {}", aggregateId, e);
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }
  }

  private ResponseEntity<?> checkRouting(String aggregateId, boolean forwarded, String path, String authorization) {
    KafkaStreams streams = eventify.getKafkaStreams();

    if (streams.state() != KafkaStreams.State.RUNNING) {
      log.warn("Kafka Streams is not running, current state: {}", streams.state());
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }

    if (thisHost.equals(HostInfo.unavailable())) {
      return null; // single-node: query locally
    }

    KeyQueryMetadata metadata = streams.queryMetadataForKey(EVENT_STORE, aggregateId, Serdes.String().serializer());
    if (metadata == null || metadata.activeHost().equals(HostInfo.unavailable())) {
      log.warn("Metadata unavailable for aggregate {}: {}", aggregateId, metadata);
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }

    HostInfo activeHost = metadata.activeHost();
    if (activeHost.equals(thisHost)) {
      return null; // owned by this node: query locally
    }

    if (forwarded) {
      log.warn("Aggregate {} not owned by this node after forwarding, possible rebalance in progress", aggregateId);
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }

    try {
      log.debug("Forwarding request for aggregate {} to {}", aggregateId, activeHost);
      String url = UriComponentsBuilder.newInstance()
          .scheme("http")
          .host(activeHost.host())
          .port(activeHost.port())
          .path(path)
          .queryParam("forwarded", true)
          .buildAndExpand(aggregateId)
          .toUriString();
      Object result = restClient.get()
          .uri(url)
          .headers(h -> { if (authorization != null) h.set("Authorization", authorization); })
          .retrieve()
          .body(Object.class);
      return ResponseEntity.ok(result);
    } catch (Exception e) {
      log.warn("Failed to forward aggregate {} to {}", aggregateId, activeHost, e);
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }
  }
}
