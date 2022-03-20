package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.example.domain.CustomerCommand;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerResultHandler;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
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
import static io.github.alikelleci.eventify.messaging.Metadata.TIMESTAMP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

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
        .registerHandler(new CustomerEventSourcingHandler())
        .registerHandler(new CustomerEventHandler())
        .registerHandler(new CustomerResultHandler());

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
    Command command = Command.builder()
        .payload(CreateCustomer.builder()
            .id("customer-123")
            .firstName("Peter")
            .lastName("Bruin")
            .credits(100)
            .birthday(Instant.now())
            .build())
        .metadata(new Metadata()
            .add("custom-key", "custom-value")
            .add(CORRELATION_ID, UUID.randomUUID().toString())
            .add(ID, "should-be-overwritten-by-command-id")
            .add(TIMESTAMP, "should-be-overwritten-by-command-timestamp")
            .add(RESULT, "should-be-overwritten-by-command-result")
            .add(CAUSE, "should-be-overwritten-by-command-result"))
        .build();

    commands.pipeInput(command.getAggregateId(), command);

    // Assert Command Metadata
    assertThat(command.getMetadata().get(ID), is(notNullValue()));
    assertThat(command.getMetadata().get(ID), is(command.getId()));
    assertThat(command.getMetadata().get(ID), is(command.getMetadata().getMessageId()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(notNullValue()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(command.getTimestamp().toString()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(command.getMetadata().getTimestamp().toString()));

    // Assert Event
    Event event = events.readValue();

    assertThat(event, is(notNullValue()));
    assertThat(event.getId(), startsWith(command.getAggregateId()));
    assertThat(event.getAggregateId(), is(command.getAggregateId()));
    assertThat(event.getTimestamp(), is(command.getTimestamp()));
    // Metadata
    assertThat(event.getMetadata(), is(notNullValue()));
    assertThat(event.getMetadata().get("custom-key"), is("custom-value"));
    assertThat(event.getMetadata().get(CORRELATION_ID), is(notNullValue()));
    assertThat(event.getMetadata().get(CORRELATION_ID), is(command.getMetadata().get(CORRELATION_ID)));
    assertThat(event.getMetadata().get(ID), is(notNullValue()));
    assertThat(event.getMetadata().get(ID), is(event.getId()));
    assertThat(event.getMetadata().get(ID), is(event.getMetadata().getMessageId()));
    assertThat(event.getMetadata().get(TIMESTAMP), is(notNullValue()));
    assertThat(event.getMetadata().get(TIMESTAMP), is(event.getTimestamp().toString()));
    assertThat(event.getMetadata().get(TIMESTAMP), is(event.getMetadata().getTimestamp().toString()));
    // Payload
    assertThat(event.getPayload(), instanceOf(CustomerCreated.class));
    assertThat(((CustomerCreated) event.getPayload()).getId(), is(((CreateCustomer) command.getPayload()).getId()));
    assertThat(((CustomerCreated) event.getPayload()).getLastName(), is(((CreateCustomer) command.getPayload()).getLastName()));
    assertThat(((CustomerCreated) event.getPayload()).getCredits(), is(((CreateCustomer) command.getPayload()).getCredits()));
    assertThat(((CustomerCreated) event.getPayload()).getBirthday(), is(((CreateCustomer) command.getPayload()).getBirthday()));

    // Assert Command result
    Command commandResult = commandResults.readValue();

    assertThat(commandResult, is(notNullValue()));
    assertThat(commandResult.getId(), startsWith(command.getAggregateId()));
    assertThat(commandResult.getAggregateId(), is(command.getAggregateId()));
    assertThat(commandResult.getTimestamp(), is(command.getTimestamp()));
    // Metadata
    assertThat(commandResult.getMetadata(), is(notNullValue()));
    assertThat(commandResult.getMetadata().get("custom-key"), is("custom-value"));
    assertThat(commandResult.getMetadata().get(CORRELATION_ID), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(CORRELATION_ID), is(command.getMetadata().get(CORRELATION_ID)));
    assertThat(commandResult.getMetadata().get(ID), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(ID), is(commandResult.getId()));
    assertThat(commandResult.getMetadata().get(ID), is(commandResult.getMetadata().getMessageId()));
    assertThat(commandResult.getMetadata().get(TIMESTAMP), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(TIMESTAMP), is(commandResult.getTimestamp().toString()));
    assertThat(commandResult.getMetadata().get(TIMESTAMP), is(commandResult.getMetadata().getTimestamp().toString()));
    assertThat(commandResult.getMetadata().get(RESULT), is("success"));
    assertThat(commandResult.getMetadata().get(CAUSE), isEmptyOrNullString());
    // Payload
    assertThat(commandResult.getPayload(), is(command.getPayload()));
  }


}
