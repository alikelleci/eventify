package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.example.domain.CustomerCommand;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import io.github.alikelleci.eventify.util.CommonUtils;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class EventifyTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, Event> events;

  @BeforeEach
  void setup() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventify-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);


    Eventify eventify = new EventifyBuilder(props)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
        .build();

    testDriver = new TopologyTestDriver(eventify.buildTopology(), props);

    commands = testDriver.createInputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), CustomSerdes.Json(Command.class).serializer());

    events = testDriver.createOutputTopic(CustomerEvent.class.getAnnotation(TopicInfo.class).value(),
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
    CreateCustomer payload = CreateCustomer.builder()
        .id("customer-123")
        .firstName("Peter")
        .lastName("Bruin")
        .build();

    String aggregateId = CommonUtils.getAggregateId(payload);
    String messageId = CommonUtils.createMessageId(aggregateId);

    commands.pipeInput(aggregateId, Command.builder()
        .id(messageId)
        .timestamp(Instant.now())
        .aggregateId(aggregateId)
        .payload(payload)
        .build());

    // Result
    Event event = events.readValue();

    assertThat(event, is(notNullValue()));
    assertThat(event.getAggregateId(), is(aggregateId));
    assertThat(event.getPayload(), instanceOf(CustomerCreated.class));
    assertThat(((CustomerCreated) event.getPayload()).getFirstName(), is(payload.getFirstName()));
    assertThat(((CustomerCreated) event.getPayload()).getLastName(), is(payload.getLastName()));
    assertThat(((CustomerCreated) event.getPayload()).getCredits(), is(payload.getCredits()));
    assertThat(((CustomerCreated) event.getPayload()).getBirthday(), is(payload.getBirthday()));
  }
}
