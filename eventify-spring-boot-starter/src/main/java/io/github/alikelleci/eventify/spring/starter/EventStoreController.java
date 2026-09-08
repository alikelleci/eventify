package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
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
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

@Slf4j
@RestController
@RequestMapping("/eventify")
public class EventStoreController {

  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);

  private static final String EVENT_STORE = "event-store";
  // '~' is near the top of printable ASCII, so aggregateId@~ upper-bounds a prefix scan on aggregateId@
  private static final String KEY_RANGE_PREFIX = "@";
  private static final String KEY_RANGE_SUFFIX = "@~";

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
  }

  @GetMapping("/aggregates/{aggregateId}/events")
  public ResponseEntity<List<Event>> getEvents(@PathVariable("aggregateId") String aggregateId,
                                               @RequestParam(name = "forwarded", defaultValue = "false") boolean forwarded) {
    KafkaStreams streams = eventify.getKafkaStreams();

    if (streams.state() != KafkaStreams.State.RUNNING) {
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }

    KeyQueryMetadata metadata = streams.queryMetadataForKey(EVENT_STORE, aggregateId, Serdes.String().serializer());

    if (metadata == null || metadata.activeHost().equals(HostInfo.unavailable())) {
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }

    HostInfo activeHost = metadata.activeHost();

    if (!forwarded && !thisHost.equals(HostInfo.unavailable()) && !activeHost.equals(thisHost)) {
      try {
        log.debug("Forwarding request for aggregate {} to {}", aggregateId, activeHost);
        String url = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host(activeHost.host())
            .port(activeHost.port())
            .path("/eventify/aggregates/{aggregateId}/events")
            .queryParam("forwarded", true)
            .buildAndExpand(aggregateId)
            .toUriString();
        List<Event> result = restClient.get()
            .uri(url)
            .retrieve()
            .body(new ParameterizedTypeReference<>() {});
        return ResponseEntity.ok(result);
      } catch (Exception e) {
        log.warn("Failed to forward aggregate {} to {}", aggregateId, activeHost, e);
        return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
      }
    }

    try {
      ReadOnlyKeyValueStore<String, Event> store = streams
          .store(StoreQueryParameters.fromNameAndType(EVENT_STORE, QueryableStoreTypes.keyValueStore()));

      List<Event> events = new ArrayList<>();
      try (KeyValueIterator<String, Event> it = store.range(aggregateId + KEY_RANGE_PREFIX, aggregateId + KEY_RANGE_SUFFIX)) {
        it.forEachRemaining(kv -> events.add(kv.value));
      }
      return ResponseEntity.ok(events);
    } catch (Exception e) {
      log.debug("Event store temporarily unavailable for aggregate {}", aggregateId, e);
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }
  }
}
