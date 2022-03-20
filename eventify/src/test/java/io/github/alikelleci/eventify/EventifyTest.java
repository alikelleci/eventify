package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.example.domain.CustomerCommand;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.messaging.Metadata;
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
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class EventifyTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, Command> commandResults;
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

    Eventify eventify = new Eventify(props)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler());

    testDriver = new TopologyTestDriver(eventify.topology(), props);

    commands = testDriver.createInputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), CustomSerdes.Json(Command.class).serializer());

    commandResults = testDriver.createOutputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value().concat(".results"),
        new StringDeserializer(), CustomSerdes.Json(Command.class).deserializer());

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
        .credits(100)
        .birthday(Instant.now())
        .build();

    String aggregateId = CommonUtils.getAggregateId(payload);
    String messageId = CommonUtils.createMessageId(aggregateId);
    Instant timestamp = Instant.now();
    String correlationId = UUID.randomUUID().toString();

    commands.pipeInput(CommonUtils.getAggregateId(payload), Command.builder()
        .id(messageId)
        .timestamp(timestamp)
        .aggregateId(CommonUtils.getAggregateId(payload))
        .payload(payload)
        .metadata(Metadata.builder()
            .entry(Metadata.CORRELATION_ID, correlationId)
            .build())
        .build());

    // Assert Event
    Event event = events.readValue();

    assertThat(event, is(notNullValue()));
    assertThat(event.getAggregateId(), is(aggregateId));
    assertThat(event.getTimestamp(), is(timestamp));
    assertThat(event.getMetadata(), is(notNullValue()));
    assertThat(event.getMetadata().get(Metadata.CORRELATION_ID), is(correlationId));

    assertThat(event.getPayload(), instanceOf(CustomerCreated.class));
    assertThat(((CustomerCreated) event.getPayload()).getId(), is(payload.getId()));
    assertThat(((CustomerCreated) event.getPayload()).getFirstName(), is(payload.getFirstName()));
    assertThat(((CustomerCreated) event.getPayload()).getLastName(), is(payload.getLastName()));
    assertThat(((CustomerCreated) event.getPayload()).getCredits(), is(payload.getCredits()));
    assertThat(((CustomerCreated) event.getPayload()).getBirthday(), is(payload.getBirthday()));

    // Assert Command result
    Command commandResult = commandResults.readValue();

    assertThat(commandResult, is(notNullValue()));
    assertThat(commandResult.getAggregateId(), is(aggregateId));
    assertThat(commandResult.getTimestamp(), is(timestamp));
    assertThat(commandResult.getMetadata(), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(Metadata.CORRELATION_ID), is(correlationId));
    assertThat(commandResult.getMetadata().get(Metadata.RESULT), is("success"));
    assertThat(commandResult.getPayload(), is(payload));
  }


}
