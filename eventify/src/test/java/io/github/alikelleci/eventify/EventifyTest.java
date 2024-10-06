package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.example.domain.Customer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsIssued;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.support.serializer.JsonDeserializer;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.github.alikelleci.eventify.factory.CommandFactory.buildAddCreditsCommand;
import static io.github.alikelleci.eventify.factory.CommandFactory.buildCreateCustomerCommand;
import static io.github.alikelleci.eventify.factory.CommandFactory.buildIssueCreditsCommand;
import static io.github.alikelleci.eventify.factory.CommandFactory.faker;
import static io.github.alikelleci.eventify.factory.EventFactory.generateEventsFor;
import static io.github.alikelleci.eventify.util.Matchers.assertCommandResult;
import static io.github.alikelleci.eventify.util.Matchers.assertEvent;
import static io.github.alikelleci.eventify.util.Matchers.assertSnapshot;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
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
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    Eventify eventify = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
//        .registerHandler(new CustomerEventHandler())
//        .registerHandler(new CustomerResultHandler())
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
  class BasicTests {

    @Test
    void commandShouldSucceed() {
      List<Command> commands = List.of(
          buildCreateCustomerCommand("cust-1", "John", "Doe", 100)
      );
      commands.forEach(command -> commandsTopic.pipeInput(command.getAggregateId(), command));

      List<Command> commandResults = commandResultsTopic.readValuesToList();
      assertThat(commandResults.size(), is(commands.size()));

      assertCommandResult(commands.get(0), commandResults.get(0), true);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events.size(), is(1));

      assertEvent(commands.get(0), events.get(0), CustomerCreated.class);
      assertThat(((CustomerCreated) events.get(0).getPayload()).getId(), is(((CreateCustomer) commands.get(0).getPayload()).getId()));
      assertThat(((CustomerCreated) events.get(0).getPayload()).getFirstName(), is(((CreateCustomer) commands.get(0).getPayload()).getFirstName()));
      assertThat(((CustomerCreated) events.get(0).getPayload()).getLastName(), is(((CreateCustomer) commands.get(0).getPayload()).getLastName()));
      assertThat(((CustomerCreated) events.get(0).getPayload()).getCredits(), is(((CreateCustomer) commands.get(0).getPayload()).getCredits()));
      assertThat(((CustomerCreated) events.get(0).getPayload()).getBirthday(), is(((CreateCustomer) commands.get(0).getPayload()).getBirthday()));

      List<Event> eventsInStore = readEventsFromStore();
      assertThat(eventsInStore.size(), is(1));
      assertThat(eventsInStore, containsInRelativeOrder(events.toArray(new Event[0])));
    }

    @Test
    void commandShouldFail() {
      List<Command> commands = List.of(
          buildAddCreditsCommand("cust-1", 100)
      );
      commands.forEach(command -> commandsTopic.pipeInput(command.getAggregateId(), command));

      List<Command> commandResults = commandResultsTopic.readValuesToList();
      assertThat(commandResults.size(), is(commands.size()));

      assertCommandResult(commands.get(0), commandResults.get(0), false);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events.size(), is(0));

      List<Event> eventsInStore = readEventsFromStore();
      assertThat(eventsInStore.size(), is(0));
    }

  }

  @Nested
  class AdvancedTests {

    @Test
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
      assertThat(commandResults.size(), is(commands.size()));

      assertCommandResult(commands.get(0), commandResults.get(0), true);
      assertCommandResult(commands.get(1), commandResults.get(1), true);
      assertCommandResult(commands.get(2), commandResults.get(2), true);
      assertCommandResult(commands.get(3), commandResults.get(3), true);
      assertCommandResult(commands.get(4), commandResults.get(4), true);
      assertCommandResult(commands.get(5), commandResults.get(5), false);
      assertCommandResult(commands.get(6), commandResults.get(6), false);
      assertCommandResult(commands.get(7), commandResults.get(7), true);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events.size(), is(6));

      assertEvent(commands.get(0), events.get(0), CustomerCreated.class);
      assertEvent(commands.get(1), events.get(1), CreditsAdded.class);
      assertEvent(commands.get(2), events.get(2), CreditsAdded.class);
      assertEvent(commands.get(3), events.get(3), CreditsAdded.class);
      assertEvent(commands.get(4), events.get(4), CreditsAdded.class);
      assertEvent(commands.get(7), events.get(5), CreditsIssued.class);

      List<Event> eventsInStore = readEventsFromStore();
      assertThat(eventsInStore.size(), is(6));
      assertThat(eventsInStore, containsInRelativeOrder(events.toArray(new Event[0])));

      Aggregate snapshot = snapshotStore.get("cust-1");
      assertThat(snapshot, is(notNullValue()));

      assertSnapshot(events.get(4), snapshot, Customer.class, 5);
      assertThat(((Customer) snapshot.getPayload()).getId(), is("cust-1"));
      assertThat(((Customer) snapshot.getPayload()).getFirstName(), is("John"));
      assertThat(((Customer) snapshot.getPayload()).getLastName(), is("Doe"));
      assertThat(((Customer) snapshot.getPayload()).getCredits(), is(104));
      assertThat(((Customer) snapshot.getPayload()).getBirthday(), is(notNullValue()));
    }
  }

  @Disabled
  @Nested
  class PerformanceTests {

    @Test
    void test1() {
      int numberOfAggregates = 1000;
      int numberOfEventsPerAggregate = 1000;

      for (int i = 1; i <= numberOfAggregates; i++) {
        generateEventsFor("cust-" + i, numberOfEventsPerAggregate, true, event ->
            eventStore.put(event.getId(), event));
      }
      log.info("Number of events generated: {}", numberOfAggregates * numberOfEventsPerAggregate);

      for (int i = 1; i <= 4; i++) {
        int number = faker.number().numberBetween(1, numberOfAggregates);
        String aggregateId = "cust-" + number;
        sendCommandsAndLogExecutionTime(aggregateId, 4);
        log.info("------------------------------------------------------");
      }

      log.info("Number of events (approx.) in store: {}", eventStore.approximateNumEntries());
    }

    private void sendCommandsAndLogExecutionTime(String aggregateId, int totalCommands) {
      log.info("Sending {} command(s) for: {}", totalCommands, aggregateId);
      for (int i = 1; i <= totalCommands; i++) {
        StopWatch stopWatch = StopWatch.createStarted();
        Command command = buildAddCreditsCommand(aggregateId, 1);
        commandsTopic.pipeInput(command.getAggregateId(), command);
        stopWatch.stop();
        log.info("Command {} execution time: {} milliseconds ({} seconds)", i, stopWatch.getTime(TimeUnit.MILLISECONDS), stopWatch.getTime(TimeUnit.SECONDS));
      }
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


  private List<Aggregate> readSnapshotsFromStore() {
    return IteratorUtils.toList(snapshotStore.all())
        .stream().map(keyValue -> keyValue.value)
        .toList();
  }

  private Aggregate readSnapshotFromStore(String aggregateId) {
    return readSnapshotsFromStore().stream()
        .filter(aggregate -> aggregate.getAggregateId().equals(aggregateId))
        .filter(aggregate -> aggregate.getId().startsWith(aggregateId + "@"))
        .findFirst()
        .orElse(null);
  }
}
