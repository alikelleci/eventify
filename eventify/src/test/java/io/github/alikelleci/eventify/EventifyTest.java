package io.github.alikelleci.eventify;

import com.github.javafaker.Faker;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.example.domain.Customer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.AddCredits;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.eventify.example.domain.CustomerCommand.IssueCredits;
import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CreditsIssued;
import io.github.alikelleci.eventify.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerResultHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerUpcaster;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.support.serializer.JsonDeserializer;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.common.serialization.Serdes;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.github.alikelleci.eventify.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.RESULT;
import static io.github.alikelleci.eventify.messaging.Metadata.TIMESTAMP;
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
  void test1() {
    Command command = Command.builder()
        .payload(CreateCustomer.builder()
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

    commandsTopic.pipeInput(command.getAggregateId(), command);

    // Assert Command Metadata
    assertThat(command.getMetadata().get(ID), is(notNullValue()));
    assertThat(command.getMetadata().get(ID), is(command.getId()));
    assertThat(command.getMetadata().get(ID), is(command.getMetadata().getMessageId()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(notNullValue()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(command.getTimestamp().toString()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(command.getMetadata().getTimestamp().toString()));

    // Assert Command Result
    Command commandResult = commandResultsTopic.readValue();
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
//    assertThat(commandResult.getMetadata().get(CAUSE), isEmptyOrNullString());
    // Payload
    assertThat(commandResult.getPayload(), is(command.getPayload()));

    // Assert Event
    Event event = eventsTopic.readValue();
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
//    assertThat(event.getMetadata().get(RESULT), isEmptyOrNullString());
//    assertThat(event.getMetadata().get(CAUSE), isEmptyOrNullString());
    // Payload
    assertThat(event.getPayload(), instanceOf(CustomerCreated.class));
    assertThat(((CustomerCreated) event.getPayload()).getId(), is(((CreateCustomer) command.getPayload()).getId()));
    assertThat(((CustomerCreated) event.getPayload()).getLastName(), is(((CreateCustomer) command.getPayload()).getLastName()));
    assertThat(((CustomerCreated) event.getPayload()).getCredits(), is(((CreateCustomer) command.getPayload()).getCredits()));
    assertThat(((CustomerCreated) event.getPayload()).getBirthday(), is(((CreateCustomer) command.getPayload()).getBirthday()));
  }

  @Test
  void test2() {
    List<Command> commands = new ArrayList<>();

    // CreateCustomer
    commands.add(Command.builder()
        .payload(CreateCustomer.builder()
            .id("customer-123")
            .firstName("Peter")
            .lastName("Bruin")
            .credits(100)
            .birthday(Instant.now())
            .build())
        .build());

    //  AddCredits
    commands.add(Command.builder()
        .payload(AddCredits.builder()
            .id("customer-123")
            .amount(25)
            .build())
        .build());

    // IssueCredits
    commands.add(Command.builder()
        .payload(IssueCredits.builder()
            .id("customer-123")
            .amount(100)
            .build())
        .build());

    // AddCredits
    commands.add(Command.builder()
        .payload(AddCredits.builder()
            .id("customer-123")
            .amount(5)
            .build())
        .build());

    // IssueCredits: this command should be rejected (total credits = 30)
    commands.add(Command.builder()
        .payload(IssueCredits.builder()
            .id("customer-123")
            .amount(31)
            .build())
        .build());

    // Send commands
    commands.forEach(command ->
        commandsTopic.pipeInput(command.getAggregateId(), command));

    List<KeyValue<String, Event>> events = IteratorUtils.toList(eventStore.all());

    // Assert Event Store
    assertThat(events.size(), is(4));
    assertThat(events.get(0).value.getPayload(), instanceOf(CustomerCreated.class));
    assertThat(events.get(1).value.getPayload(), instanceOf(CreditsAdded.class));
    assertThat(events.get(2).value.getPayload(), instanceOf(CreditsIssued.class));
    assertThat(events.get(3).value.getPayload(), instanceOf(CreditsAdded.class));
  }

  @Test
  void test3() {
    List<Command> commands = new ArrayList<>();

    // CreateCustomer
    commands.add(Command.builder()
        .payload(CreateCustomer.builder()
            .id("customer-123")
            .firstName("Peter")
            .lastName("Bruin")
            .credits(100)
            .birthday(Instant.now())
            .build())
        .build());

    // AddCredits
    commands.add(Command.builder()
        .payload(AddCredits.builder()
            .id("customer-123")
            .amount(25)
            .build())
        .build());

    // AddCredits
    commands.add(Command.builder()
        .payload(AddCredits.builder()
            .id("customer-123")
            .amount(25)
            .build())
        .build());

    // AddCredits
    commands.add(Command.builder()
        .payload(AddCredits.builder()
            .id("customer-123")
            .amount(25)
            .build())
        .build());

    // IssueCredits -> Snapshot point
    commands.add(Command.builder()
        .payload(IssueCredits.builder()
            .id("customer-123")
            .amount(25)
            .build())
        .build());

    // AddCredits
    commands.add(Command.builder()
        .payload(AddCredits.builder()
            .id("customer-123")
            .amount(25)
            .build())
        .build());

    // Send commands
    commands.forEach(command ->
        commandsTopic.pipeInput(command.getAggregateId(), command));

    List<KeyValue<String, Event>> events = IteratorUtils.toList(eventStore.all());
    List<KeyValue<String, Aggregate>> snapshots = IteratorUtils.toList(snapshotStore.all());

    // Assert Snapshot Store
    assertThat(snapshots.size(), is(1));
    assertThat(snapshots.get(0).value.getAggregateId(), is("customer-123"));
    assertThat(snapshots.get(0).value.getEventId(), is(events.get(4).key));
    assertThat(snapshots.get(0).value.getVersion(), is(5L));
    assertThat(snapshots.get(0).value.getPayload(), instanceOf(Customer.class));
    assertThat(((Customer) snapshots.get(0).value.getPayload()).getCredits(), is(150));
  }


  @Test
  void bla() {
    List<Command> commands = new ArrayList<>();

    // CreateCustomer
    commands.add(Command.builder()
        .payload(CreateCustomer.builder()
            .id("customer-123")
            .firstName("Peter")
            .lastName("Bruin")
            .credits(100)
            .birthday(Instant.now())
            .build())
        .build());

    // Generate random commands
    commands.addAll(generateCommands(10));

    // Send commands
    commands.forEach(command ->
        commandsTopic.pipeInput(command.getAggregateId(), command));

    // Assert Command Results
    List<Command> commandResults = commandResultsTopic.readValuesToList();
    assertThat(commandResults.size(), is(commands.size()));

    // Accepted Commands
    List<Command> acceptedCommands = commandResults.stream()
        .filter(command -> command.getMetadata().get(RESULT).equals("success"))
        .collect(Collectors.toList());

    if (acceptedCommands.isEmpty()) {
      assertThat(eventsTopic.isEmpty(), is(true));
    } else {
      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events.size(), is(acceptedCommands.size()));
    }

    // Rejected Commands
    List<Command> rejectedCommands = commandResults.stream()
        .filter(command -> command.getMetadata().get(RESULT).equals("failure"))
        .collect(Collectors.toList());
  }

  private List<Command> generateCommands(int numberOfCommands) {
    List<Command> commands = new ArrayList<>();

    for (int i = 0; i < numberOfCommands; i++) {
      Class<?> type = faker.options().option(AddCredits.class, IssueCredits.class);

      if (type == AddCredits.class) {
        commands.add(Command.builder()
            .payload(AddCredits.builder()
                .id("customer-123")
                .amount(faker.number().numberBetween(1, 100))
                .build())
            .metadata(Metadata.builder()
                .add(CORRELATION_ID, UUID.randomUUID().toString())
                .build())
            .build());
      }

      if (type == IssueCredits.class) {
        commands.add(Command.builder()
            .payload(IssueCredits.builder()
                .id("customer-123")
                .amount(faker.number().numberBetween(1, 100))
                .build())
            .metadata(Metadata.builder()
                .add(CORRELATION_ID, UUID.randomUUID().toString())
                .build())
            .build());
      }
    }

    return commands;
  }

}
