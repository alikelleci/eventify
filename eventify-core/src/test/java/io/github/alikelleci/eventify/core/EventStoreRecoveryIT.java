package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.TopicNames;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.command.gateway.CommandGateway;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The event store over restarts and over rebalances between two instances. Every event must continue its aggregate's
 * sequence and carry the state its command handler saw: the state the handling instance had rebuilt.
 */
@Testcontainers
@DisplayName("Event store recovery (exactly-once, real broker)")
class EventStoreRecoveryIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final String COMMANDS = "rec.commands";

  @AggregateRoot("counter")
  @EnableSnapshotting(threshold = 3)
  @Value
  @Builder
  public static class Counter {
    @AggregateId
    String id;
    int count;
    long sum;
  }

  @Topic(COMMANDS)
  @Value
  @Builder
  public static class Add {
    @AggregateId
    String id;
    int value;
  }

  /** Carries the state the command handler saw: what the instance that handled it had rebuilt. */
  @Topic("rec.events")
  @Value
  @Builder
  public static class Added {
    @AggregateId
    String id;
    int value;
    int countBefore;
    long sumBefore;
  }

  public static class CounterHandler {
    @CommandHandler
    public List<Added> handle(Add command, Counter state) {
      int count = state != null ? state.getCount() : 0;
      long sum = state != null ? state.getSum() : 0;
      // Two events per command, so snapshots are stepped over as well as hit.
      return List.of(
          Added.builder().id(command.getId()).value(command.getValue()).countBefore(count).sumBefore(sum).build(),
          Added.builder().id(command.getId()).value(1).countBefore(count + 1).sumBefore(sum + command.getValue()).build());
    }

    @EventSourcingHandler
    public Counter handle(Added event, Counter state) {
      int count = state != null ? state.getCount() : 0;
      long sum = state != null ? state.getSum() : 0;
      return Counter.builder().id(event.getId()).count(count + 1).sum(sum + event.getValue()).build();
    }
  }

  @TempDir
  Path stateDir;

  private Eventify first;
  private Eventify second;
  private CommandGateway gateway;

  /** The model: per aggregate how many events it has and their sum. */
  private final Map<String, long[]> model = new HashMap<>();

  @BeforeAll
  static void createTopics() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(
          new NewTopic(COMMANDS, 4, (short) 1),
          new NewTopic(TopicNames.scheduledTopicOf(COMMANDS), 4, (short) 1),
          new NewTopic(TopicNames.resultTopicOf(COMMANDS), 4, (short) 1),
          new NewTopic("rec.events", 4, (short) 1),
          new NewTopic("rec.replies", 1, (short) 1))).all().get();
    }
  }

  @AfterEach
  void tearDown() {
    if (gateway != null) gateway.close();
    if (first != null) first.stop();
    if (second != null) second.stop();
  }

  @Test
  @DisplayName("Should continue every aggregate after a restart, with an empty state directory and from its checkpoint")
  void restartWithAnEmptyStateDirectory() {
    first = start("rec-restart", stateDir.resolve("one"));
    gateway = gateway();
    sendRounds("r", 7);

    first.stop();
    first = start("rec-restart", stateDir.resolve("two")); // nothing local: all from the changelogs
    sendRounds("r", 5);

    first.stop();
    first = start("rec-restart", stateDir.resolve("two")); // the same directory: from its checkpoint
    sendRounds("r", 3);
  }

  @Test
  @DisplayName("Should continue every aggregate when a second instance joins, the first leaves and comes back")
  void rebalanceBetweenTwoInstances() {
    first = start("rec-rebalance", stateDir.resolve("a"));
    gateway = gateway();
    sendRounds("b", 5);

    second = start("rec-rebalance", stateDir.resolve("b")); // joins: takes over some partitions
    await(() -> first.getKafkaStreams().state() == KafkaStreams.State.RUNNING && second.getKafkaStreams().state() == KafkaStreams.State.RUNNING);
    sendRounds("b", 5);
    assertThat(second.getKafkaStreams().metadataForLocalThreads().stream().mapToInt(thread -> thread.activeTasks().size()).sum())
        .as("tasks of the second instance").isGreaterThan(0);

    first.stop(); // leaves: the second takes over the rest after the session timeout
    sendRounds("b", 5);

    first = start("rec-rebalance", stateDir.resolve("a")); // back, with its old state directory
    await(() -> first.getKafkaStreams().state() == KafkaStreams.State.RUNNING && second.getKafkaStreams().state() == KafkaStreams.State.RUNNING);
    sendRounds("b", 5);
  }

  /** Each round one command to each of 12 aggregates; every event must continue the model exactly. */
  private void sendRounds(String prefix, int rounds) {
    for (int round = 0; round < rounds; round++) {
      for (int n = 0; n < 12; n++) {
        String id = prefix + "-" + n;
        long[] expected = model.computeIfAbsent(id, key -> new long[2]);
        int value = (int) (expected[0] * 7 + n + 3);
        List<Event> events = gateway.sendAndWait(Add.builder().id(id).value(value).build(), 90, TimeUnit.SECONDS);
        assertThat(events).hasSize(2);
        for (Event event : events) {
          Added added = (Added) event.getPayload();
          assertThat(event.getSequence()).as("sequence of %s", id).isEqualTo(expected[0] + 1);
          assertThat(added.getCountBefore()).as("count seen for %s", id).isEqualTo((int) expected[0]);
          assertThat(added.getSumBefore()).as("sum seen for %s", id).isEqualTo(expected[1]);
          expected[0]++;
          expected[1] += added.getValue();
        }
      }
    }
  }

  private Eventify start(String applicationId, Path stateDirectory) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 6000);
    Eventify instance = Eventify.builder().streamsConfig(properties).registerHandler(new CounterHandler()).build();
    instance.start();
    await(() -> instance.getKafkaStreams().state() == KafkaStreams.State.RUNNING);
    return instance;
  }

  private static CommandGateway gateway() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    return CommandGateway.builder().producerConfig(producerConfig).replyTopic("rec.replies").build();
  }

  private static void await(java.util.function.BooleanSupplier condition) {
    Instant deadline = Instant.now().plusSeconds(120);
    while (!condition.getAsBoolean()) {
      if (Instant.now().isAfter(deadline)) throw new AssertionError("Timed out");
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
    }
  }
}
