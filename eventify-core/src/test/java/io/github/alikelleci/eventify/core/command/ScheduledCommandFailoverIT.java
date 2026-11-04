package io.github.alikelleci.eventify.core.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.command.gateway.CommandGateway;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Scheduled commands through downtime, failover, two applications and a backlog, under exactly-once, against a real broker. */
@Testcontainers
@DisplayName("Scheduled commands: downtime, failover and load (exactly-once, real broker)")
class ScheduledCommandFailoverIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final String COMMANDS = "ops.commands";
  private static final String RESULTS = TopicNames.resultTopicOf(COMMANDS);
  private static final ObjectMapper JSON = EventifyObjectMapper.create();

  @AggregateRoot("reminder")
  @Value
  @Builder
  public static class Reminder {
    @AggregateId
    String id;
  }

  @AggregateRoot("snoozer")
  @Value
  @Builder
  public static class Snoozer {
    @AggregateId
    String id;
  }

  @Topic(COMMANDS)
  @Value
  @Builder
  public static class Remind {
    @AggregateId
    String id;
  }

  /** Handled by another application, on the same command topic. */
  @Topic(COMMANDS)
  @Value
  @Builder
  public static class Snooze {
    @AggregateId
    String id;
  }

  @Topic("ops.events")
  @Value
  @Builder
  public static class Reminded {
    @AggregateId
    String id;
  }

  @Topic("ops.events")
  @Value
  @Builder
  public static class Snoozed {
    @AggregateId
    String id;
  }

  public static class ReminderHandler {
    @CommandHandler
    public Object handle(Remind command, Reminder state) {
      return Reminded.builder().id(command.getId()).build();
    }

    @EventSourcingHandler
    public Reminder handle(Reminded event, Reminder state) {
      return Reminder.builder().id(event.getId()).build();
    }
  }

  public static class SnoozeHandler {
    @CommandHandler
    public Object handle(Snooze command, Snoozer state) {
      return Snoozed.builder().id(command.getId()).build();
    }

    @EventSourcingHandler
    public Snoozer handle(Snoozed event, Snoozer state) {
      return Snoozer.builder().id(event.getId()).build();
    }
  }

  @TempDir
  Path stateDir;

  private final List<Eventify> instances = new ArrayList<>();
  private CommandGateway gateway;

  @BeforeAll
  static void createTopics() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(
          new NewTopic(COMMANDS, 2, (short) 1),
          new NewTopic(TopicNames.scheduledTopicOf(COMMANDS), 2, (short) 1),
          new NewTopic(RESULTS, 2, (short) 1),
          new NewTopic("ops.events", 2, (short) 1),
          new NewTopic("ops.replies", 1, (short) 1))).all().get();
    }
  }

  @AfterEach
  void tearDown() {
    if (gateway != null) gateway.close();
    instances.forEach(Eventify::stop);
  }

  /** Delivered once it runs again, from the store it left behind. */
  @Test
  @DisplayName("Should handle a command that became due while Eventify was down, once, when it runs again")
  void handlesACommandThatBecameDueWhileDown() {
    Path dir = stateDir.resolve("downtime");
    Eventify first = start("ops-downtime", dir, new ReminderHandler());
    gateway = gateway();

    Instant deliverAt = Instant.now().plusSeconds(3);
    Command command = Command.builder().payload(Remind.builder().id("down-1").build()).build();
    gateway.schedule(command, deliverAt).join();
    await("the command to be stored", () -> committedCount("ops-downtime-scheduled-commands-changelog") > 0);
    first.stop();
    instances.remove(first);

    sleepUntil(deliverAt.plusSeconds(3));
    assertThat(resultsPerCommand()).as("handled while down").doesNotContainKey(command.getId());

    start("ops-downtime", dir, new ReminderHandler());
    await("the result", () -> resultsPerCommand().containsKey(command.getId()));
    sleep(Duration.ofSeconds(3));
    assertThat(resultsPerCommand().get(command.getId())).as("results").isEqualTo(1);
  }

  /** The other instance takes over the stopped one's partitions and restores its store from the changelog. */
  @Test
  @DisplayName("Should handle every command once when one of two instances stops before they are due")
  void handlesEveryCommandOnceWhenAnInstanceStops() {
    start("ops-failover", stateDir.resolve("one"), new ReminderHandler());
    Eventify second = start("ops-failover", stateDir.resolve("two"), new ReminderHandler());
    gateway = gateway();

    Instant now = Instant.now();
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Command command = Command.builder().payload(Remind.builder().id("fail-" + i).build()).build();
      gateway.schedule(command, now.plusSeconds(8 + i % 4)).join();
      ids.add(command.getId());
    }
    await("all commands to be stored", () -> committedCount("ops-failover-scheduled-commands-changelog") >= 20);
    second.stop();
    instances.remove(second);

    await("every result", () -> resultsPerCommand().keySet().containsAll(ids));
    sleep(Duration.ofSeconds(3));
    Map<String, Integer> results = resultsPerCommand();
    ids.forEach(id -> assertThat(results.get(id)).as("results of " + id).isEqualTo(1));
  }

  /** Both read the scheduled topic; each delivers only the commands it has a handler for. */
  @Test
  @DisplayName("Should handle each command once when two applications with different handlers share the command topic")
  void handlesEachCommandOnceWithTwoApplications() {
    start("ops-reminders", stateDir.resolve("reminders"), new ReminderHandler());
    start("ops-snoozes", stateDir.resolve("snoozes"), new SnoozeHandler());
    gateway = gateway();

    Instant deliverAt = Instant.now().plusSeconds(4);
    Command remind = Command.builder().payload(Remind.builder().id("app-1").build()).build();
    Command snooze = Command.builder().payload(Snooze.builder().id("app-2").build()).build();
    gateway.schedule(remind, deliverAt).join();
    gateway.schedule(snooze, deliverAt).join();

    await("both results", () -> resultsPerCommand().keySet().containsAll(List.of(remind.getId(), snooze.getId())));
    sleep(Duration.ofSeconds(4));
    Map<String, Integer> results = resultsPerCommand();
    assertThat(results.get(remind.getId())).as("results of the reminder").isEqualTo(1);
    assertThat(results.get(snooze.getId())).as("results of the snooze").isEqualTo(1);
  }

  /** More due at once than one round delivers, on each partition: delivered over several rounds, each once. */
  @Test
  @DisplayName("Should handle a backlog larger than one round, every command once")
  void handlesABacklogEveryCommandOnce() {
    start("ops-backlog", stateDir.resolve("backlog"), new ReminderHandler());
    gateway = gateway();

    Instant deliverAt = Instant.now().plusSeconds(15);
    List<String> ids = new ArrayList<>();
    List<CompletableFuture<ScheduleToken>> scheduled = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      Command command = Command.builder().payload(Remind.builder().id("load-" + i).build()).build();
      scheduled.add(gateway.schedule(command, deliverAt));
      ids.add(command.getId());
    }
    CompletableFuture.allOf(scheduled.toArray(CompletableFuture[]::new)).join();
    await("all commands to be stored", () -> committedCount("ops-backlog-scheduled-commands-changelog") >= 3000);
    assertThat(Instant.now()).as("stored before they are due").isBefore(deliverAt);

    await("every result", () -> resultsPerCommand().keySet().containsAll(ids));
    sleep(Duration.ofSeconds(3));
    Map<String, Integer> results = resultsPerCommand();
    assertThat(ids).allSatisfy(id -> assertThat(results.get(id)).as("results of " + id).isEqualTo(1));
  }

  private Eventify start(String applicationId, Path stateDirectory, Object handler) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    Eventify instance = Eventify.builder().streamsConfig(properties).registerHandler(handler).build();
    instance.start();
    instances.add(instance);
    await("Eventify to run", () -> instance.getKafkaStreams().state() == KafkaStreams.State.RUNNING);
    return instance;
  }

  private static CommandGateway gateway() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    return CommandGateway.builder().producerConfig(producerConfig).replyTopic("ops.replies").build();
  }

  /** How many committed results each command has. */
  private static Map<String, Integer> resultsPerCommand() {
    Map<String, Integer> found = new HashMap<>();
    readCommitted(RESULTS, record -> {
      try {
        String id = JSON.readTree(record.value()).path("command").path("id").asText();
        found.merge(id, 1, Integer::sum);
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    });
    return found;
  }

  private static int committedCount(String topic) {
    int[] count = {0};
    readCommitted(topic, record -> count[0]++);
    return count[0];
  }

  private static void readCommitted(String topic, Consumer<ConsumerRecord<String, String>> each) {
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new StringDeserializer())) {
      List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
          .map(info -> new TopicPartition(topic, info.partition())).toList();
      if (partitions.isEmpty()) {
        return;
      }
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
      while (partitions.stream().anyMatch(partition -> consumer.position(partition) < end.get(partition))) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
          if (record.value() != null && record.offset() < end.get(new TopicPartition(record.topic(), record.partition()))) {
            each.accept(record);
          }
        }
      }
    }
  }

  private static void await(String what, BooleanSupplier condition) {
    Instant deadline = Instant.now().plusSeconds(120);
    while (!condition.getAsBoolean()) {
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      sleep(Duration.ofMillis(500));
    }
  }

  private static void sleepUntil(Instant moment) {
    Duration left = Duration.between(Instant.now(), moment);
    if (!left.isNegative()) {
      sleep(left);
    }
  }

  private static void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
