package io.github.alikelleci.eventify.messaging.commandhandling;

import com.github.javafaker.Faker;
import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerResultHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerUpcaster;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

class CommandTransformerTest {

  private TopologyTestDriver testDriver;
  private CommandTransformer commandTransformer;
  private MockProcessorContext context;
  private Faker faker = new Faker();

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
    context = new MockProcessorContext();

    KeyValueStore<String, Event> eventStore = (TimestampedKeyValueStore) testDriver.getTimestampedKeyValueStore("event-store");
    context.register(eventStore, null);

    KeyValueStore<String, Aggregate> snapshotStore = (TimestampedKeyValueStore) testDriver.getTimestampedKeyValueStore("snapshot-store");
    context.register(snapshotStore, null);

    commandTransformer = new CommandTransformer(eventify);
    commandTransformer.init(context);
  }

  @AfterEach
  void tearDown() {
    if (testDriver != null) {
      testDriver.close();
    }
  }

  @Test
  void transform() {
  }

  @Test
  void loadAggregate() {
    commandTransformer.saveEvent(Event.builder()
        .payload(CustomerCreated.builder()
            .id("customer-123")
            .firstName("Peter")
            .lastName("Bruin")
            .credits(100)
            .birthday(Instant.now())
            .build())
        .build());

    commandTransformer.saveEvent(Event.builder()
        .payload(CustomerEvent.CreditsIssued.builder()
            .id("customer-123")
            .amount(25)
            .build())
        .build());

//    context.setRecordTimestamp(Instant.now().toEpochMilli());


    Aggregate aggregate = commandTransformer.loadAggregate("customer-123");
    System.out.println(aggregate);
  }
}