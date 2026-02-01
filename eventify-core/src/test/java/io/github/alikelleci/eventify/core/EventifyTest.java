package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.example.customer.core.Customer;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerCommand;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CreditsIssued;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.core.example.customer.core.CustomerCommandHandler;
import io.github.alikelleci.eventify.core.example.customer.core.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static io.github.alikelleci.eventify.core.factory.CommandFactory.buildAddCreditsCommand;
import static io.github.alikelleci.eventify.core.factory.CommandFactory.buildCreateCustomerCommand;
import static io.github.alikelleci.eventify.core.factory.CommandFactory.buildIssueCreditsCommand;
import static io.github.alikelleci.eventify.core.util.Matchers.assertCommandResult;
import static io.github.alikelleci.eventify.core.util.Matchers.assertEvent;
import static io.github.alikelleci.eventify.core.util.Matchers.assertSnapshot;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class EventifyTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Command> commandsTopic;
  private TestOutputTopic<String, Command> commandResultsTopic;
  private TestOutputTopic<String, Event> eventsTopic;

  private KeyValueStore<String, Event> eventStore;
  private KeyValueStore<String, AggregateState> snapshotStore;

  @BeforeEach
  void setup() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "example-app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    Eventify eventify = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
//        .registerHandler(new CustomerEventHandler())
//        .registerHandler(new CustomerResultHandler())
//        .registerHandler(new CustomerEventUpcaster())
        .build();

    testDriver = new TopologyTestDriver(eventify.topology());

    commandsTopic = testDriver.createInputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), new JsonSerializer<>());

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
  @DisplayName("Basic Tests")
  class BasicTests {

    @Test
    @DisplayName("Should process command and produce event")
    void commandShouldSucceed() {
      List<Command> commands = List.of(
          buildCreateCustomerCommand("cust-1", "John", "Doe", 100)
      );
      commands.forEach(command -> commandsTopic.pipeInput(command.getAggregateId(), command));

      List<Command> commandResults = commandResultsTopic.readValuesToList();
      assertThat(commandResults).hasSize(commands.size());

      assertCommandResult(commands.get(0), commandResults.get(0), true);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events).hasSize(1);

      assertEvent(commands.get(0), events.get(0), CustomerCreated.class);
      assertThat(((CustomerCreated) events.get(0).getPayload()).getId()).isEqualTo(((CreateCustomer) commands.get(0).getPayload()).getId());
      assertThat(((CustomerCreated) events.get(0).getPayload()).getFirstName()).isEqualTo(((CreateCustomer) commands.get(0).getPayload()).getFirstName());
      assertThat(((CustomerCreated) events.get(0).getPayload()).getLastName()).isEqualTo(((CreateCustomer) commands.get(0).getPayload()).getLastName());
      assertThat(((CustomerCreated) events.get(0).getPayload()).getCredits()).isEqualTo(((CreateCustomer) commands.get(0).getPayload()).getCredits());
      assertThat(((CustomerCreated) events.get(0).getPayload()).getBirthday()).isEqualTo(((CreateCustomer) commands.get(0).getPayload()).getBirthday());

      List<Event> eventsInStore = readEventsFromStore();
      assertThat(eventsInStore).hasSize(1);
      assertThat(eventsInStore).containsExactlyElementsOf(events);
    }

    @Test
    @DisplayName("Should handle command validation failure")
    void commandShouldFail() {
      List<Command> commands = List.of(
          buildAddCreditsCommand("cust-1", 100)
      );
      commands.forEach(command -> commandsTopic.pipeInput(command.getAggregateId(), command));

      List<Command> commandResults = commandResultsTopic.readValuesToList();
      assertThat(commandResults).hasSize(commands.size());

      assertCommandResult(commands.get(0), commandResults.get(0), false);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events).isEmpty();

      List<Event> eventsInStore = readEventsFromStore();
      assertThat(eventsInStore).isEmpty();
    }

  }

  @Nested
  @DisplayName("Advanced Tests")
  class AdvancedTests {

    @Test
    @DisplayName("Should process multiple command and produce events")
    void multipleCommandShouldSucceed() {
      List<Command> commands = List.of(
          buildCreateCustomerCommand("cust-1", "John", "Doe", 100),
          buildAddCreditsCommand("cust-1", 1),
          buildAddCreditsCommand("cust-1", 1),
          buildAddCreditsCommand("cust-1", 1),
          buildAddCreditsCommand("cust-1", 1), // --> snapshot threshold reached, will be created on next command
          buildCreateCustomerCommand("cust-1", "John", "Doe", 100),
          buildIssueCreditsCommand("cust-1", 200), // should fail
          buildIssueCreditsCommand("cust-1", 2)
      );
      commands.forEach(command -> commandsTopic.pipeInput(command.getAggregateId(), command));

      List<Command> commandResults = commandResultsTopic.readValuesToList();
      assertThat(commandResults).hasSize(commands.size());

      assertCommandResult(commands.get(0), commandResults.get(0), true);
      assertCommandResult(commands.get(1), commandResults.get(1), true);
      assertCommandResult(commands.get(2), commandResults.get(2), true);
      assertCommandResult(commands.get(3), commandResults.get(3), true);
      assertCommandResult(commands.get(4), commandResults.get(4), true);
      assertCommandResult(commands.get(5), commandResults.get(5), false);
      assertCommandResult(commands.get(6), commandResults.get(6), false);
      assertCommandResult(commands.get(7), commandResults.get(7), true);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events).hasSize(6);

      assertEvent(commands.get(0), events.get(0), CustomerCreated.class);
      assertEvent(commands.get(1), events.get(1), CreditsAdded.class);
      assertEvent(commands.get(2), events.get(2), CreditsAdded.class);
      assertEvent(commands.get(3), events.get(3), CreditsAdded.class);
      assertEvent(commands.get(4), events.get(4), CreditsAdded.class);
      assertEvent(commands.get(7), events.get(5), CreditsIssued.class);

      List<Event> eventsInStore = readEventsFromStore();
      assertThat(eventsInStore).hasSize(6);
      assertThat(eventsInStore).containsExactlyElementsOf(events);

      AggregateState state = snapshotStore.get("cust-1");
      assertThat(state).isNotNull();

      assertSnapshot(events.get(4), state, Customer.class, 5);
      assertThat(((Customer) state.getPayload()).getId()).isEqualTo("cust-1");
      assertThat(((Customer) state.getPayload()).getFirstName()).isEqualTo("John");
      assertThat(((Customer) state.getPayload()).getLastName()).isEqualTo("Doe");
      assertThat(((Customer) state.getPayload()).getCredits()).isEqualTo(104);
      assertThat(((Customer) state.getPayload()).getBirthday()).isNotNull();
    }
  }

  private List<Event> readEventsFromStore() {
    return IteratorUtils.toList(eventStore.all())
        .stream().map(keyValue -> keyValue.value)
        .toList();
  }

  private List<Event> readEventsFromStore(String aggregateId) {
    return readEventsFromStore().stream()
        .filter(event -> event.getAggregateId().equals(aggregateId))
        .filter(event -> event.getId().startsWith(aggregateId + "@"))
        .toList();
  }


  private List<AggregateState> readSnapshotsFromStore() {
    return IteratorUtils.toList(snapshotStore.all())
        .stream().map(keyValue -> keyValue.value)
        .toList();
  }

  private AggregateState readSnapshotFromStore(String aggregateId) {
    return readSnapshotsFromStore().stream()
        .filter(state -> state.getAggregateId().equals(aggregateId))
        .filter(state -> state.getId().startsWith(aggregateId + "@"))
        .findFirst()
        .orElse(null);
  }
}
