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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Scheduled commands through the gateway, under exactly-once, against a real broker. */
@Testcontainers
@DisplayName("Scheduled commands (exactly-once, real broker)")
class ScheduledCommandIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final String COMMANDS = "reminders";
  private static final String RESULTS = TopicNames.resultTopicOf(COMMANDS);
  private static final ObjectMapper JSON = EventifyObjectMapper.create();

  @AggregateRoot("reminder")
  @Value
  @Builder
  public static class Reminder {
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

  @Topic("reminders.events")
  @Value
  @Builder
  public static class Reminded {
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

  @AggregateRoot("snoozer")
  @Value
  @Builder
  public static class Snoozer {
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

  @Topic("reminders.events")
  @Value
  @Builder
  public static class Snoozed {
    @AggregateId
    String id;
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

  private Eventify eventify;
  private Eventify other;
  private CommandGateway gateway;

  @BeforeAll
  static void createTopics() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(
          new NewTopic(COMMANDS, 2, (short) 1),
          new NewTopic(TopicNames.scheduledTopicOf(COMMANDS), 2, (short) 1),
          new NewTopic(RESULTS, 2, (short) 1),
          new NewTopic("reminders.events", 2, (short) 1),
          new NewTopic("reminders.replies", 1, (short) 1))).all().get();
    }
  }

  @AfterEach
  void tearDown() {
    if (gateway != null) gateway.close();
    if (eventify != null) eventify.stop();
    if (other != null) other.stop();
  }

  @Test
  @DisplayName("Should complete the future once Kafka has the command, and handle the command once at its time")
  void handlesTheCommandOnceAtItsTime() {
    eventify = start("scheduled-on-time", stateDir.resolve("a"));
    gateway = gateway();

    Instant deliverAt = Instant.now().plusSeconds(5);
    Command command = Command.builder().payload(Remind.builder().id("rem-1").build()).build();
    assertThat(gateway.schedule(command, deliverAt)).succeedsWithin(Duration.ofSeconds(10));
    assertThat(committedResults(command.getId())).as("handled right after scheduling").isEmpty();

    await("the result", () -> !committedResults(command.getId()).isEmpty());
    List<Long> results = committedResults(command.getId());
    assertThat(results).hasSize(1);
    assertThat(results.get(0)).as("not handled before its time").isGreaterThanOrEqualTo(deliverAt.toEpochMilli());

    sleep(Duration.ofSeconds(3));
    assertThat(committedResults(command.getId())).as("handled again").hasSize(1);
  }

  @Test
  @DisplayName("Should handle commands of several aggregates, over both partitions, each once and not before its time")
  void handlesManyCommandsEachOnceAtTheirTime() {
    eventify = start("scheduled-many", stateDir.resolve("many"));
    gateway = gateway();

    Instant now = Instant.now();
    Map<String, Instant> deliverAt = new HashMap<>();
    for (int i = 0; i < 20; i++) {
      Command command = Command.builder().payload(Remind.builder().id("rem-many-" + i).build()).build();
      Instant at = now.plusSeconds(2 + i % 5);
      assertThat(gateway.schedule(command, at)).succeedsWithin(Duration.ofSeconds(10));
      deliverAt.put(command.getId(), at);
    }

    await("every result", () -> committedResultsPerCommand().keySet().containsAll(deliverAt.keySet()));
    sleep(Duration.ofSeconds(3));
    Map<String, List<Long>> all = committedResultsPerCommand();
    deliverAt.forEach((id, at) -> {
      List<Long> results = all.getOrDefault(id, List.of());
      assertThat(results).as("results of " + id).hasSize(1);
      assertThat(results.get(0)).as("not handled before its time: " + id).isGreaterThanOrEqualTo(at.toEpochMilli());
    });
  }

  @Test
  @DisplayName("Should not handle a cancelled command, and still handle the others")
  void doesNotHandleACancelledCommand() {
    eventify = start("scheduled-cancel", stateDir.resolve("cancel"));
    gateway = gateway();

    Instant deliverAt = Instant.now().plusSeconds(5);
    Command cancelled = Command.builder().payload(Remind.builder().id("rem-cancel-1").build()).build();
    Command kept = Command.builder().payload(Remind.builder().id("rem-cancel-2").build()).build();
    ScheduleToken token = gateway.schedule(cancelled, deliverAt).join();
    gateway.schedule(kept, deliverAt).join();
    assertThat(gateway.cancel(token)).succeedsWithin(Duration.ofSeconds(10));

    await("the kept command's result", () -> !committedResults(kept.getId()).isEmpty());
    sleep(Duration.ofSeconds(3));
    assertThat(committedResults(cancelled.getId())).as("results of the cancelled command").isEmpty();
    assertThat(committedResults(kept.getId())).as("results of the kept command").hasSize(1);
  }

  /** Both read the scheduled topic; each delivers only the commands it has a handler for. */
  @Test
  @DisplayName("Should handle each command once when two applications with different handlers share the command topic")
  void handlesEachCommandOnceWithTwoApplications() {
    eventify = start("scheduled-reminders", stateDir.resolve("reminders"));
    other = start("scheduled-snoozes", stateDir.resolve("snoozes"), new SnoozeHandler());
    gateway = gateway();

    Instant deliverAt = Instant.now().plusSeconds(4);
    Command remind = Command.builder().payload(Remind.builder().id("app-1").build()).build();
    Command snooze = Command.builder().payload(Snooze.builder().id("app-2").build()).build();
    gateway.schedule(remind, deliverAt).join();
    gateway.schedule(snooze, deliverAt).join();

    await("both results", () -> !committedResults(remind.getId()).isEmpty() && !committedResults(snooze.getId()).isEmpty());
    sleep(Duration.ofSeconds(4));
    assertThat(committedResults(remind.getId())).as("results of the reminder").hasSize(1);
    assertThat(committedResults(snooze.getId())).as("results of the snooze").hasSize(1);
  }

  /** The store is restored from its changelog: a fresh state directory has nothing of it locally. */
  @Test
  @DisplayName("Should still deliver a scheduled command after a restart with an empty state directory")
  void survivesARestart() {
    eventify = start("scheduled-restart", stateDir.resolve("first"));
    gateway = gateway();

    Command command = Command.builder().payload(Remind.builder().id("rem-2").build()).build();
    assertThat(gateway.schedule(command, Instant.now().plusSeconds(15))).succeedsWithin(Duration.ofSeconds(10));
    await("the command to be stored", () -> committedCount("scheduled-restart-scheduled-commands-changelog") > 0);
    eventify.stop();

    eventify = start("scheduled-restart", stateDir.resolve("second"));
    await("the result", () -> !committedResults(command.getId()).isEmpty());

    sleep(Duration.ofSeconds(3));
    assertThat(committedResults(command.getId())).as("results").hasSize(1);
  }

  private Eventify start(String applicationId, Path stateDirectory) {
    return start(applicationId, stateDirectory, new ReminderHandler());
  }

  private Eventify start(String applicationId, Path stateDirectory, Object handler) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    // A stopped instance doesn't leave the group: the broker waits this long before reassigning its partitions.
    properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 6000);
    Eventify instance = Eventify.builder().streamsConfig(properties).registerHandler(handler).build();
    instance.start();
    await("Eventify to run", () -> instance.getKafkaStreams().state() == KafkaStreams.State.RUNNING);
    return instance;
  }

  private static CommandGateway gateway() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    return CommandGateway.builder().producerConfig(producerConfig).replyTopic("reminders.replies").build();
  }

  /** The timestamps of the committed results, per command id: one read for all commands. */
  private static Map<String, List<Long>> committedResultsPerCommand() {
    Map<String, List<Long>> found = new HashMap<>();
    readCommitted(RESULTS, record -> {
      if (record.value() != null) {
        try {
          String id = JSON.readTree(record.value()).path("command").path("id").asText();
          found.computeIfAbsent(id, key -> new ArrayList<>()).add(record.timestamp());
        } catch (Exception e) {
          throw new AssertionError(e);
        }
      }
    });
    return found;
  }

  /** The timestamps of the committed results of this command. */
  private static List<Long> committedResults(String commandId) {
    List<Long> found = new ArrayList<>();
    readCommitted(RESULTS, record -> {
      if (record.value() != null && record.value().contains(commandId)) {
        found.add(record.timestamp());
      }
    });
    return found;
  }

  private static int committedCount(String topic) {
    List<Object> found = new ArrayList<>();
    readCommitted(topic, record -> {
      if (record.value() != null) found.add(record);
    });
    return found.size();
  }

  private static void readCommitted(String topic, Consumer<ConsumerRecord<String, String>> each) {
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
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
          if (record.offset() < end.get(new TopicPartition(record.topic(), record.partition()))) {
            each.accept(record);
          }
        }
      }
    }
  }

  private static void await(String what, BooleanSupplier condition) {
    Instant deadline = Instant.now().plusSeconds(90);
    while (!condition.getAsBoolean()) {
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      sleep(Duration.ofMillis(200));
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
