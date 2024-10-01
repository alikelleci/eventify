package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.example.domain.CustomerCommand;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventUpcaster;
import io.github.alikelleci.eventify.example.handlers.CustomerResultHandler;
import io.github.alikelleci.eventify.factory.CommandFactory;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.support.serializer.JsonDeserializer;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.TIMESTAMP;
import static io.github.alikelleci.eventify.util.Matchers.assertEvent;
import static io.github.alikelleci.eventify.util.Matchers.assertFailedCommandResult;
import static io.github.alikelleci.eventify.util.Matchers.assertSuccessfulCommandResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

class EventifyTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Command> commandsTopic;
  private TestOutputTopic<String, Command> commandResultsTopic;
  private TestOutputTopic<String, Event> eventsTopic;

  private KeyValueStore<String, Event> eventStore;
  private KeyValueStore<String, Aggregate> snapshotStore;

  @BeforeEach
  void setup() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "example-app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");

    Eventify eventify = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
        .registerHandler(new CustomerEventHandler())
        .registerHandler(new CustomerResultHandler())
        .registerHandler(new CustomerEventUpcaster())
        .build();

    testDriver = new TopologyTestDriver(eventify.topology(), eventify.getStreamsConfig());

    commandsTopic = testDriver.createInputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), new JsonSerializer<>(Command.class));

    commandResultsTopic = testDriver.createOutputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value().concat(".results"),
        new StringDeserializer(), new JsonDeserializer<>(Command.class));

    eventsTopic = testDriver.createOutputTopic(CustomerEvent.class.getAnnotation(TopicInfo.class).value(),
        new StringDeserializer(), new JsonDeserializer<>(Event.class));

    eventStore = testDriver.getKeyValueStore("event-store");
    snapshotStore = testDriver.getKeyValueStore("snapshot-store");
  }

  @AfterEach
  void tearDown() {
    if (testDriver != null) {
      testDriver.close();
    }
  }

  @Test
  void metadataShouldSetCorrectly() {
    Command command = CommandFactory.buildCreateCustomerCommand("cust-1");

    assertThat(command.getMetadata().get(ID), is(notNullValue()));
    assertThat(command.getMetadata().get(ID), startsWith("cust-1@"));
    assertThat(command.getMetadata().get(ID), is(command.getId()));
    assertThat(command.getMetadata().get(ID), is(command.getMetadata().getMessageId()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(notNullValue()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(command.getTimestamp().toString()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(command.getMetadata().getTimestamp().toString()));
  }

  @Test
  void commandShouldSucceed() {
    Command command = CommandFactory.buildCreateCustomerCommand("cust-1");
    commandsTopic.pipeInput(command.getAggregateId(), command);

    Command commandResult = commandResultsTopic.readValue();
    assertThat(commandResult, is(notNullValue()));
    assertSuccessfulCommandResult(command, commandResult);

    Event event = eventsTopic.readValue();
    assertThat(event, is(notNullValue()));
    assertEvent(command, event, CustomerCreated.class);

    assertThat(((CustomerCreated) event.getPayload()).getId(), is(((CreateCustomer) command.getPayload()).getId()));
    assertThat(((CustomerCreated) event.getPayload()).getFirstName(), is(((CreateCustomer) command.getPayload()).getFirstName()));
    assertThat(((CustomerCreated) event.getPayload()).getLastName(), is(((CreateCustomer) command.getPayload()).getLastName()));
    assertThat(((CustomerCreated) event.getPayload()).getCredits(), is(((CreateCustomer) command.getPayload()).getCredits()));
    assertThat(((CustomerCreated) event.getPayload()).getBirthday(), is(((CreateCustomer) command.getPayload()).getBirthday()));

    List<KeyValue<String, Event>> events = IteratorUtils.toList(eventStore.all());
    assertThat(events.size(), is(1));
    assertThat(events.get(0).value.getPayload(), instanceOf(CustomerCreated.class));
  }

  @Test
  void commandShouldFail() {
    Command command = CommandFactory.buildAddCreditsCommand("cust-1");
    commandsTopic.pipeInput(command.getAggregateId(), command);

    Command commandResult = commandResultsTopic.readValue();
    assertThat(commandResult, is(notNullValue()));
    assertFailedCommandResult(command, commandResult, "ValidationException: Customer does not exists.");

    assertThat(eventsTopic.isEmpty(), is(true));

    List<KeyValue<String, Event>> events = IteratorUtils.toList(eventStore.all());
    assertThat(events.size(), is(0));
  }

}
