package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.example.domain.CustomerEvent;
import io.github.alikelleci.eventify.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.eventify.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.commandhandling.gateway.CommandGateway;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventhandling.gateway.EventGateway;
import io.github.alikelleci.eventify.support.serializer.JsonDeserializer;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.alikelleci.eventify.factory.CommandFactory.buildAddCreditsCommand;
import static io.github.alikelleci.eventify.factory.CommandFactory.faker;

@Slf4j
public class EventifyBenchmark {

  private static final Eventify eventify = createEventify();
  private static final CommandGateway commandGateway = createCommandGateway();
  private static final EventGateway eventGateway = createEventGateway();

  private static final Producer<String, Message> producer = createProducer();
  private static final Consumer<String, Message> consumer = createConsumer();

  public static final int NUMBER_OF_AGGREGATES = 1000;
  public static final int NUMBER_OF_EVENTS_PER_AGGREGATE = 1000;

  public static AtomicBoolean isReady = new AtomicBoolean(false);

  @BeforeAll
  static void setup() {
    createTopics();
    generateEvents();
    eventify.start();

    while (!isReady.get()) {
      // state restoration in progress...
    }
    log.info("Setup complete.");
  }

  @AfterAll
  static void tearDown() {
    eventify.stop();
    eventify.getKafkaStreams().cleanUp();
    producer.close();
    consumer.close();
    deleteTopics();
  }

  @Test
  void test1() throws ExecutionException, InterruptedException {
    for (int i = 1; i <= 4; i++) {
      int number = faker.number().numberBetween(1, NUMBER_OF_AGGREGATES);
      String aggregateId = "cust-" + number;
      sendCommandsAndLogExecutionTime(aggregateId, 4);
      log.info("------------------------------------------------------");
    }
  }

  public static void generateEvents() {
    String topic = "benchmark-app-event-store-changelog";
    Event event;

    for (int i = 1; i <= NUMBER_OF_AGGREGATES; i++) {
      event = Event.builder()
          .payload(CustomerEvent.CustomerCreated.builder()
              .id("cust-" + i)
              .firstName("John " + i)
              .lastName("Doe " + i)
              .credits(100)
              .build())
          .build();

      producer.send(new ProducerRecord<>(topic, event.getId(), event));

      for (int j = 1; j <= NUMBER_OF_EVENTS_PER_AGGREGATE; j++) {
        event = Event.builder()
            .payload(CustomerEvent.CreditsAdded.builder()
                .id("cust-" + i)
                .amount(1)
                .build())
            .build();

        producer.send(new ProducerRecord<>(topic, event.getId(), event));
      }

      if (i % 10_000 == 0) {
        log.info("Number of customers created: {}", i);
      }
    }

    producer.flush();
    log.info("All done.");
  }

  private void sendCommandsAndLogExecutionTime(String aggregateId, int totalCommands) throws ExecutionException, InterruptedException {
    log.info("Sending {} command(s) for: {}", totalCommands, aggregateId);
    for (int i = 1; i <= totalCommands; i++) {
      StopWatch stopWatch = StopWatch.createStarted();
      Command command = buildAddCreditsCommand(aggregateId, 1);
      commandGateway.send(command.getPayload()).get();
      stopWatch.stop();
      log.info("Command {} execution time: {} milliseconds ({} seconds)", i, stopWatch.getTime(TimeUnit.MILLISECONDS), stopWatch.getTime(TimeUnit.SECONDS));
    }
  }

  public static Eventify createEventify() {
    Properties streamsConfig = new Properties();
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "benchmark-app");
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\tmp\\kafka-streams");
    streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);

    return Eventify.builder()
        .streamsConfig(streamsConfig)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
        .stateListener((newState, oldState) -> {
          log.warn("State changed from {} to {}", oldState, newState);
          if (newState == KafkaStreams.State.RUNNING) {
            isReady.set(true);
          }
        })
        .build();
  }

  public static CommandGateway createCommandGateway() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    return CommandGateway.builder()
        .producerConfig(properties)
        .replyTopic("my-reply-channel")
        .build();
  }

  public static EventGateway createEventGateway() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    return EventGateway.builder()
        .producerConfig(properties)
        .build();
  }

  public static Producer<String, Message> createProducer() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "benchmark-consumer");
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    try (AdminClient adminClient = AdminClient.create(properties)) {
      adminClient.createTopics(List.of(
              new NewTopic("commands.customer", 1, (short) 1),
              new NewTopic("events.customer", 1, (short) 1)
          ))
          .all().get();
    }
  }

  @SneakyThrows
  public static void deleteTopics() {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    try (AdminClient adminClient = AdminClient.create(properties)) {
      Set<String> topics = adminClient.listTopics().names().get();
      if (CollectionUtils.isNotEmpty(topics)) {
        adminClient.deleteTopics(topics).all().get();
      }
    }
  }

}
