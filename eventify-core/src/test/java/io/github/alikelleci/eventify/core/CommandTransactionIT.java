package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.internal.StoreKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a failing command leaves committed under exactly-once, against a real broker. A failure after a write must reach
 * Kafka Streams so the transaction aborts; a caught failure is committed with what was written before it.
 */
@Testcontainers
@DisplayName("Command transactions (exactly-once, real broker)")
class CommandTransactionIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  @AggregateRoot("order")
  @EnableSnapshotting(threshold = 1, deleteEvents = true)
  @Value
  @Builder(toBuilder = true)
  public static class Order {
    @AggregateId
    String id;
  }

  @Topic("orders")
  public interface OrderCommand {
  }

  @Topic("orders.events")
  public interface OrderEvent {
  }

  /** Scenario 1: returns a valid event and an {@link EventWithoutTopic}. */
  @Value
  @Builder
  public static class EmitEventWithoutTopic implements OrderCommand {
    @AggregateId
    String id;
  }

  /** Scenario 2: returns an {@link EventThatFailsWhenSent}. */
  @Value
  @Builder
  public static class EmitEventThatFailsWhenSent implements OrderCommand {
    @AggregateId
    String id;
  }

  /** A valid event, with a topic. */
  @Value
  @Builder
  public static class OrderPlaced implements OrderEvent {
    @AggregateId
    String id;
  }

  /** A user mistake: no {@code @Topic}, so it has no topic to be sent to. */
  @Value
  @Builder
  public static class EventWithoutTopic {
    @AggregateId
    String id;
  }

  /** Fails on the {@link #FAIL_ON_WRITE}th JSON write; set to the last one, it fails when the event is sent. */
  @Value
  @Builder
  public static class EventThatFailsWhenSent implements OrderEvent {
    static final AtomicInteger TIMES_WRITTEN = new AtomicInteger();
    static volatile int FAIL_ON_WRITE = Integer.MAX_VALUE;
    /** Only this aggregate's writes count: commands of earlier tests are replayed from the shared topic too. */
    static volatile String COUNTED_ID;

    @AggregateId
    String id;

    public String getContent() {
      if (id.equals(COUNTED_ID) && TIMES_WRITTEN.incrementAndGet() == FAIL_ON_WRITE) {
        throw new IllegalStateException("cannot be written this time");
      }
      return "content";
    }
  }

  public static class OrderHandler {
    @HandleCommand
    public Object handle(EmitEventWithoutTopic command, Order state) {
      return List.of(OrderPlaced.builder().id(command.getId()).build(), EventWithoutTopic.builder().id(command.getId()).build());
    }

    @HandleCommand
    public Object handle(EmitEventThatFailsWhenSent command, Order state) {
      return EventThatFailsWhenSent.builder().id(command.getId()).build();
    }

    @ApplyEvent
    public Order apply(OrderPlaced event, Order state) {
      return Order.builder().id(event.getId()).build();
    }

    @ApplyEvent
    public Order apply(EventWithoutTopic event, Order state) {
      return state;
    }

    @ApplyEvent
    public Order apply(EventThatFailsWhenSent event, Order state) {
      return Order.builder().id(event.getId()).build();
    }
  }

  @TempDir
  Path stateDir;

  private Eventify eventify;

  @BeforeAll
  static void createTopics() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(
          new NewTopic("orders", 1, (short) 1),
          new NewTopic("orders.results", 1, (short) 1),
          new NewTopic("orders.events", 1, (short) 1))).all().get();
    }
  }

  @AfterEach
  void tearDown() {
    if (eventify != null) eventify.stop();
    EventThatFailsWhenSent.COUNTED_ID = null;
  }

  /** Scenario 1: the first event is valid, the second has no topic. The command fails, and none of it is stored or sent. */
  @Test
  @DisplayName("Should fail the command and commit nothing of it when one of its events has no @Topic")
  void aCommandWithAnEventWithoutATopicLeavesNothingBehind() {
    eventify = start("orders-event-without-topic");
    send(EmitEventWithoutTopic.builder().id("order-1").build());
    await("the command's result", () -> !committed("orders.results", "order-1").isEmpty());

    assertThat(committed("orders.results", "order-1")).as("committed results").containsExactly("failure");
    assertThat(committed("orders.events", "order-1")).as("committed events").isEmpty();
    assertThat(committed("orders-event-without-topic-event-store-changelog", "order-1")).as("committed event store").isEmpty();
    assertThat(committed("orders-event-without-topic-snapshot-store-changelog", "order-1")).as("committed snapshot store").isEmpty();
  }

  /** Scenario 2: sending the event fails after it was stored and the result sent; nothing of the command is committed. */
  @Test
  @DisplayName("Should abort the transaction and commit nothing when sending an event fails after the command was accepted")
  void aFailureAfterTheCommandIsAcceptedIsAborted() {
    eventify = start("orders-event-fails-when-sent");

    // First a command that succeeds, to count how often its event is written as JSON: the last time is when it is sent.
    EventThatFailsWhenSent.COUNTED_ID = "order-counting-writes";
    EventThatFailsWhenSent.TIMES_WRITTEN.set(0);
    send(EmitEventThatFailsWhenSent.builder().id("order-counting-writes").build());
    await("the counting command's result", () -> !committed("orders.results", "order-counting-writes").isEmpty());
    int writesPerCommand = EventThatFailsWhenSent.TIMES_WRITTEN.get();
    assertThat(committed("orders-event-fails-when-sent-event-store-changelog", "order-counting-writes"))
        .containsExactly(StoreKeys.event("order", "order-counting-writes", 1));
    assertThat(committed("orders-event-fails-when-sent-snapshot-store-changelog", "order-counting-writes"))
        .containsExactly(StoreKeys.aggregate("order", "order-counting-writes"));

    // The same command again, now failing on that last write.
    EventThatFailsWhenSent.COUNTED_ID = "order-2";
    EventThatFailsWhenSent.TIMES_WRITTEN.set(0);
    EventThatFailsWhenSent.FAIL_ON_WRITE = writesPerCommand;
    try {
      send(EmitEventThatFailsWhenSent.builder().id("order-2").build());
      await("the command to fail or to be handled", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.ERROR
          || !committed("orders.results", "order-2").isEmpty());
    } finally {
      EventThatFailsWhenSent.FAIL_ON_WRITE = Integer.MAX_VALUE;
    }

    assertThat(committed("orders.results", "order-2")).as("committed results").isEmpty();
    assertThat(committed("orders.events", "order-2")).as("committed events").isEmpty();
    assertThat(committed("orders-event-fails-when-sent-event-store-changelog", "order-2")).as("committed event store").isEmpty();
    assertThat(committed("orders-event-fails-when-sent-snapshot-store-changelog", "order-2")).as("committed snapshot store").isEmpty();
  }

  @Test
  @DisplayName("Should roll back a new snapshot and event pruning together when sending fails")
  void rollbackPreservesPreviousSnapshotAndHistory() {
    String applicationId = "orders-snapshot-rollback";
    String id = "order-retained";
    eventify = start(applicationId);
    EventThatFailsWhenSent.COUNTED_ID = id;
    EventThatFailsWhenSent.TIMES_WRITTEN.set(0);
    send(EmitEventThatFailsWhenSent.builder().id(id).build());
    await("the initial command to commit", () -> committed("orders.results", id).size() == 1);
    int writesPerCommand = EventThatFailsWhenSent.TIMES_WRITTEN.get();

    EventThatFailsWhenSent.TIMES_WRITTEN.set(0);
    EventThatFailsWhenSent.FAIL_ON_WRITE = writesPerCommand;
    try {
      send(EmitEventThatFailsWhenSent.builder().id(id).build());
      await("the next command to fail", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.ERROR
          || committed("orders.results", id).size() > 1);
    } finally {
      EventThatFailsWhenSent.FAIL_ON_WRITE = Integer.MAX_VALUE;
    }

    assertThat(eventify.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.ERROR);
    assertThat(committed("orders.results", id)).containsExactly("success");
    assertThat(committed("orders.events", id)).containsExactly(id);
    // Only the first committed entries survive. No new event, replacement snapshot or pruning tombstone was committed.
    assertThat(committed(applicationId + "-event-store-changelog", id)).containsExactly(StoreKeys.event("order", id, 1));
    assertThat(committed(applicationId + "-snapshot-store-changelog", id)).containsExactly(StoreKeys.aggregate("order", id));
  }

  private Eventify start(String applicationId) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    Eventify instance = Eventify.builder().streamsConfig(properties).registerHandler(new OrderHandler()).build();
    instance.start();
    await("Eventify to run", () -> instance.getKafkaStreams().state() == KafkaStreams.State.RUNNING);
    return instance;
  }

  private static void send(Object payload) {
    Command command = Command.builder().payload(payload).build();
    try (KafkaProducer<String, Command> producer = new KafkaProducer<>(
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringSerializer(), new JsonSerializer<>())) {
      producer.send(new ProducerRecord<>("orders", command.getAggregateId(), command));
    }
  }

  /** What a {@code read_committed} consumer sees: for results, "success" or "failure"; otherwise the record key. */
  private static List<String> committed(String topic, String aggregateId) {
    List<String> found = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new StringDeserializer())) {
      if (consumer.partitionsFor(topic).isEmpty()) {
        return found;
      }
      TopicPartition partition = new TopicPartition(topic, 0);
      consumer.assign(List.of(partition));
      consumer.seekToBeginning(List.of(partition));
      long end = consumer.endOffsets(List.of(partition)).get(partition);
      while (consumer.position(partition) < end) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
          if (record.offset() < end && belongsTo(record.key(), aggregateId)) {
            found.add(topic.endsWith(".results") ? result(record.value()) : record.key());
          }
        }
      }
    }
    return found;
  }

  private static boolean belongsTo(String key, String aggregateId) {
    String snapshotKey = StoreKeys.aggregate("order", aggregateId);
    return key != null && (key.equals(aggregateId) || key.equals(snapshotKey)
        || key.startsWith(snapshotKey + StoreKeys.SEPARATOR));
  }

  private static String result(String json) {
    return json.contains("\"result\":\"failure\"") ? "failure" : json.contains("\"result\":\"success\"") ? "success" : json;
  }

  private static void await(String what, BooleanSupplier condition) {
    Instant deadline = Instant.now().plusSeconds(90);
    while (!condition.getAsBoolean()) {
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }
  }
}
