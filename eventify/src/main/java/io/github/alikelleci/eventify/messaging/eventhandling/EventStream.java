package io.github.alikelleci.eventify.messaging.eventhandling;


import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingTransformer;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

@Slf4j
public class EventStream {

  public void buildStream(StreamsBuilder builder) {
    // --> Events
    KStream<String, Event> events = builder.stream(Topics.EVENTS, Consumed.with(Serdes.String(), CustomSerdes.Json(Event.class)))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null);

    // Events --> Void
    events
        .transformValues(EventTransformer::new);

    // Events --> Event Store
    events
        .transformValues(EventSourcingTransformer::new, "event-store", "snapshot-store");
  }

}
