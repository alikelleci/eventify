package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerResultHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerUpcaster;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static io.github.alikelleci.eventify.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.RESULT;
import static io.github.alikelleci.eventify.messaging.Metadata.REVISION;
import static io.github.alikelleci.eventify.messaging.Metadata.TIMESTAMP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class UpcasterTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Event> events;
  private TestOutputTopic<String, Event> eventsOut;

  @BeforeEach
  void setup() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventify-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

    Eventify eventify = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
        .registerHandler(new CustomerEventHandler())
        .registerHandler(new CustomerResultHandler())
        .registerHandler(new CustomerUpcaster())
        .build();

    testDriver = new TopologyTestDriver(eventify.topology(), properties);

    events = testDriver.createInputTopic(CustomerEvent.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), CustomSerdes.Json(Event.class).serializer());

    eventsOut = testDriver.createOutputTopic("upcasted-events",
        new StringDeserializer(), CustomSerdes.Json(Event.class).deserializer());
  }

  @AfterEach
  void tearDown() {
    if (testDriver != null) {
      testDriver.close();
    }
  }

  @Test
  void test1() {
    Event event = Event.builder()
        .payload(CustomerCreated.builder()
            .id("customer-123")
            .firstName("Peter")
            .lastName("Bruin")
            .credits(100)
            .birthday(Instant.now())
            .build())
        .metadata(Metadata.builder()
            .add("custom-key", "custom-value")
            .add(CORRELATION_ID, UUID.randomUUID().toString())
            .add(ID, "should-be-overwritten-by-command-id")
            .add(TIMESTAMP, "should-be-overwritten-by-command-timestamp")
            .add(RESULT, "should-be-overwritten-by-command-result")
            .add(CAUSE, "should-be-overwritten-by-command-result")
            .build())
        .build();

    events.pipeInput(event.getAggregateId(), event);

    // Assert Event
    Event upcastedEvent = eventsOut.readValue();
    assertThat(upcastedEvent, is(notNullValue()));
    // Metadata
    assertThat(upcastedEvent.getMetadata().get(REVISION), is(String.valueOf(4)));
    // Payload
    assertThat(((CustomerCreated) upcastedEvent.getPayload()).getFirstName(), is("John v3 -> v4"));


  }


}
