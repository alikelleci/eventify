package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.example.domain.Customer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventSourcingHandler;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.RESULT;
import static io.github.alikelleci.eventify.messaging.Metadata.TIMESTAMP;
import static io.github.alikelleci.eventify.util.Matchers.assertCommandResult;
import static io.github.alikelleci.eventify.util.Matchers.assertEvent;
import static io.github.alikelleci.eventify.util.Matchers.assertEventsInStore;
import static io.github.alikelleci.eventify.util.Matchers.assertSnapshotInStore;
import static org.hamcrest.MatcherAssert.assertThat;
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
//        .registerHandler(new CustomerEventUpcaster())
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

  @Nested
  class SimpleTests {

    @Test
    void metadataShouldSetCorrectly() {
      Command command = CommandFactory.buildCreateCustomerCommand("cust-1", 100);

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
      Command command = CommandFactory.buildCreateCustomerCommand("cust-1", 100);
      commandsTopic.pipeInput(command.getAggregateId(), command);

      Command commandResult = commandResultsTopic.readValue();
      assertThat(commandResult, is(notNullValue()));
      assertCommandResult(command, commandResult, true);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events.size(), is(1));
      assertEventsInStore(events, eventStore);

      assertEvent(command, events.get(0), CustomerCreated.class);
      assertThat(((CustomerCreated) events.get(0).getPayload()).getId(), is(((CreateCustomer) command.getPayload()).getId()));
      assertThat(((CustomerCreated) events.get(0).getPayload()).getFirstName(), is(((CreateCustomer) command.getPayload()).getFirstName()));
      assertThat(((CustomerCreated) events.get(0).getPayload()).getLastName(), is(((CreateCustomer) command.getPayload()).getLastName()));
      assertThat(((CustomerCreated) events.get(0).getPayload()).getCredits(), is(((CreateCustomer) command.getPayload()).getCredits()));
      assertThat(((CustomerCreated) events.get(0).getPayload()).getBirthday(), is(((CreateCustomer) command.getPayload()).getBirthday()));
    }

    @Test
    void commandShouldFail() {
      Command command = CommandFactory.buildAddCreditsCommand("cust-1", 100);
      commandsTopic.pipeInput(command.getAggregateId(), command);

      Command commandResult = commandResultsTopic.readValue();
      assertThat(commandResult, is(notNullValue()));
      assertCommandResult(command, commandResult, false);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events.size(), is(0));
      assertEventsInStore(events, eventStore);
    }

  }

  @Nested
  class AdvancedTests {

    @Test
    void multipleCommandShouldSucceed() {
      List<Command> commands = List.of(
          CommandFactory.buildCreateCustomerCommand("cust-1", 100),
          CommandFactory.buildAddCreditsCommand("cust-1", 1),
          CommandFactory.buildAddCreditsCommand("cust-1", 1),
          CommandFactory.buildAddCreditsCommand("cust-1", 1),
          CommandFactory.buildAddCreditsCommand("cust-1", 1), // --> snapshot threshold reached
          CommandFactory.buildCreateCustomerCommand("cust-1", 100), // should fail & a snapshot should be created on replay
          CommandFactory.buildIssueCreditsCommand("cust-1", 200) // should fail
      );

      commands.forEach(command ->
          commandsTopic.pipeInput(command.getAggregateId(), command));

      List<Command> commandResults = commandResultsTopic.readValuesToList();
      assertThat(commandResults.size(), is(commands.size()));

      assertCommandResult(commands.get(0), commandResults.get(0), true);
      assertCommandResult(commands.get(1), commandResults.get(1), true);
      assertCommandResult(commands.get(2), commandResults.get(2), true);
      assertCommandResult(commands.get(3), commandResults.get(3), true);
      assertCommandResult(commands.get(4), commandResults.get(4), true);
      assertCommandResult(commands.get(5), commandResults.get(5), false);
      assertCommandResult(commands.get(6), commandResults.get(6), false);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events.size(), is(5));
      assertEventsInStore(events, eventStore);

      assertEvent(commandResults.get(0), events.get(0), CustomerCreated.class);
      assertEvent(commandResults.get(1), events.get(1), CreditsAdded.class);
      assertEvent(commandResults.get(2), events.get(2), CreditsAdded.class);
      assertEvent(commandResults.get(3), events.get(3), CreditsAdded.class);
      assertEvent(commandResults.get(4), events.get(4), CreditsAdded.class);

      List<KeyValue<String, Aggregate>> snapshots = IteratorUtils.toList(snapshotStore.all());
      assertThat(snapshots.size(), is(1));

      assertSnapshotInStore(events.get(4), snapshotStore, Customer.class, 5);
    }

    @Test
    void multipleCommandShouldSucceed2() {
      List<Command> commands = List.of(
          CommandFactory.buildCreateCustomerCommand("cust-1", 100),
          CommandFactory.buildAddCreditsCommand("cust-1", 50),
          CommandFactory.buildAddCreditsCommand("cust-1", 50),
          CommandFactory.buildCreateCustomerCommand("cust-1", 100) // should fail
      );

      commands.forEach(command ->
          commandsTopic.pipeInput(command.getAggregateId(), command));

      List<Command> commandResults = commandResultsTopic.readValuesToList();
      assertThat(commandResults.size(), is(commands.size()));

      Map<Command, Command> map = new HashMap<>();
      for (int i = 0; i < commands.size(); i++) {
        map.put(commands.get(i), commandResults.get(i));
      }

      map.forEach((command, result) -> {
        boolean isSuccess = result.getMetadata().get(RESULT).equals("success");
        assertCommandResult(command, result, isSuccess);
      });

      List<Command> successfulCommands = commandResults.stream()
          .filter(command -> command.getMetadata().get(RESULT).equals("success"))
          .toList();

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events.size(), is(successfulCommands.size()));
      assertEventsInStore(events, eventStore);

      assertEvent(successfulCommands.get(0), events.get(0), CustomerCreated.class);
      assertEvent(successfulCommands.get(1), events.get(1), CreditsAdded.class);
      assertEvent(successfulCommands.get(2), events.get(2), CreditsAdded.class);
    }

  }

}
