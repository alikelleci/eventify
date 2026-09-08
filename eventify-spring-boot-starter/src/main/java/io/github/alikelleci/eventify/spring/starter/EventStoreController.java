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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

@Slf4j
@RestController
@RequestMapping("/eventify")
public class EventStoreController {

  private final Eventify eventify;
  private final HostInfo thisHost;
  private final RestClient restClient = RestClient.create();

  public EventStoreController(Eventify eventify) {
    this.eventify = eventify;
    String applicationServer = eventify.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, "");
    String[] parts = applicationServer.split(":");
    this.thisHost = (parts.length == 2)
        ? new HostInfo(parts[0], Integer.parseInt(parts[1]))
        : HostInfo.unavailable();
  }

  @GetMapping("/aggregates/{aggregateId}/events")
  public ResponseEntity<List<Event>> getEvents(@PathVariable String aggregateId,
                                               @RequestParam(defaultValue = "false") boolean forwarded) {
    KafkaStreams streams = eventify.getKafkaStreams();

    if (streams.state() != KafkaStreams.State.RUNNING) {
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }

    KeyQueryMetadata metadata = streams.queryMetadataForKey("event-store", aggregateId, Serdes.String().serializer());

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
          .store(StoreQueryParameters.fromNameAndType("event-store", QueryableStoreTypes.keyValueStore()));

      List<Event> events = new ArrayList<>();
      try (KeyValueIterator<String, Event> it = store.range(aggregateId + "@", aggregateId + "@~")) {
        it.forEachRemaining(kv -> events.add(kv.value));
      }
      return ResponseEntity.ok(events);
    } catch (Exception e) {
      log.debug("Event store temporarily unavailable for aggregate {}", aggregateId, e);
      return ResponseEntity.status(SERVICE_UNAVAILABLE).build();
    }
  }
}
