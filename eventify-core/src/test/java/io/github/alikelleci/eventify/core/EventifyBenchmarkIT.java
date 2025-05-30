package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.core.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.core.messaging.Message;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.gateway.CommandGateway;
import io.github.alikelleci.eventify.core.messaging.commandhandling.gateway.KafkaCommandGateway;
import io.github.alikelleci.eventify.core.messaging.eventhandling.gateway.EventGateway;
import io.github.alikelleci.eventify.core.messaging.eventhandling.gateway.KafkaEventGateway;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.alikelleci.eventify.core.factory.CommandFactory.faker;
import static io.github.alikelleci.eventify.core.factory.CommandFactory.generateCommandsFor;
import static io.github.alikelleci.eventify.core.factory.EventFactory.generateEventsFor;

@Slf4j
@Disabled
public class EventifyBenchmarkIT {

  private static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");

  private static KafkaEventify eventify;
  private static CommandGateway commandGateway;
  private static EventGateway eventGateway;

  private static Producer<String, Message> producer;
  private static Consumer<String, Message> consumer;

  public static final int NUMBER_OF_AGGREGATES = 1000;
  public static final int NUMBER_OF_EVENTS_PER_AGGREGATE = 1000;

  public static AtomicBoolean isReady = new AtomicBoolean(false);

  @BeforeAll
  static void setup() {
    kafka.start();

    eventify = createEventify();
    commandGateway = createCommandGateway();
    eventGateway = createEventGateway();
    producer = createProducer();
    consumer = createConsumer();

    createTopics();
    generateEvents(NUMBER_OF_AGGREGATES, NUMBER_OF_EVENTS_PER_AGGREGATE);

    eventify.start();
    while (!isReady.get()) {
      // state restoration in progress...
    }
  }

  @AfterAll
  static void tearDown() {
    eventify.stop();
    eventify.getKafkaStreams().cleanUp();
    producer.close();
    consumer.close();
    kafka.close();
  }

  @Test
  void test1() {
    int numOfTargetAggregates = 4;
    int numCommandsPerAggregate = 2;
    generateCommands(NUMBER_OF_AGGREGATES, numOfTargetAggregates, numCommandsPerAggregate);
  }

  @Test
  void test2() {
    int numOfTargetAggregates = 8;
    int numCommandsPerAggregate = 4;
    generateCommands(NUMBER_OF_AGGREGATES, numOfTargetAggregates, numCommandsPerAggregate);
  }

  @Test
  void test3() {
    int numOfTargetAggregates = 16;
    int numCommandsPerAggregate = 8;
    generateCommands(NUMBER_OF_AGGREGATES, numOfTargetAggregates, numCommandsPerAggregate);
  }

  private static void generateEvents(int numberOfAggregates, int numberOfEventsPerAggregate) {
    String topic = "example-app-event-store-changelog";

    log.info("Generating events...");
    for (int i = 1; i <= numberOfAggregates; i++) {
      String aggregateId = "cust-" + i;

      generateEventsFor(aggregateId, numberOfEventsPerAggregate, true)
          .forEach(event -> producer.send(new ProducerRecord<>(topic, event.getId(), event)));
    }
    producer.flush();
    log.info("Number of events generated: {}", numberOfAggregates * numberOfEventsPerAggregate);
  }

  private void generateCommands(int numOfAggregates, int numOfTargetAggregates, int numCommandsPerAggregate) {
    log.info("Generating commands...");
    for (int i = 1; i <= numOfTargetAggregates; i++) {
      String aggregateId = "cust-" + faker.number().numberBetween(1, numOfAggregates);

      log.info("Sending {} command(s) for: {}", numCommandsPerAggregate, aggregateId);
      generateCommandsFor(aggregateId, numCommandsPerAggregate, false)
          .forEach(this::sendCommandAndLogExecutionTime);
      log.info("------------------------------------------------------");
    }
    log.info("Number of commands generated: {}", numOfTargetAggregates * numCommandsPerAggregate);
  }

  @SneakyThrows
  private void sendCommandAndLogExecutionTime(Command command) {
    Instant startTime = Instant.now();
    commandGateway.send(command.getPayload()).get();
    Instant endTime = Instant.now();

    Duration duration = Duration.between(startTime, endTime);
    log.info("Command {} executed in: {} ms ({} sec)", command.getType(), duration.toMillis(), duration.toSeconds());
  }

  public static KafkaEventify createEventify() {
    Properties streamsConfig = new Properties();
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "example-app");
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\tmp\\kafka-streams");
    streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);

    return KafkaEventify.builder()
        .streamsConfig(streamsConfig)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
        .stateListener((newState, oldState) -> {
          log.debug("State changed from {} to {}", oldState, newState);
          if (newState == KafkaStreams.State.RUNNING) {
            isReady.set(true);
          }
        })
        .build();
  }

  public static CommandGateway createCommandGateway() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

    return KafkaCommandGateway.builder()
        .producerConfig(properties)
        .replyTopic("my-reply-channel")
        .build();
  }

  public static EventGateway createEventGateway() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

    return KafkaEventGateway.builder()
        .producerConfig(properties)
        .build();
  }

  public static Producer<String, Message> createProducer() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

    return new KafkaProducer<>(properties,
        new StringSerializer(),
        new JsonSerializer<>());
  }

  public static Consumer<String, Message> createConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "example-consumer");
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

    return new KafkaConsumer<>(properties,
        new StringDeserializer(),
        new JsonDeserializer<>(Message.class));
  }

  @SneakyThrows
  public static void createTopics() {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

    try (AdminClient adminClient = AdminClient.create(properties)) {
      adminClient.createTopics(List.of(
              new NewTopic("commands.customer", 1, (short) 1),
              new NewTopic("commands.customer.results", 1, (short) 1),
              new NewTopic("events.customer", 1, (short) 1)
          ))
          .all().get();
    }
  }

}
