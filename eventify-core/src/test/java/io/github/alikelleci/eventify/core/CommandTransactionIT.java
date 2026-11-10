package io.github.alikelleci.eventify.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.command.internal.HeaderNames;
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
import org.apache.kafka.common.header.internals.RecordHeader;
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

import java.nio.charset.StandardCharsets;
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
    @CommandHandler
    public Object handle(EmitEventWithoutTopic command, Order state) {
      return List.of(OrderPlaced.builder().id(command.getId()).build(), EventWithoutTopic.builder().id(command.getId()).build());
    }

    @CommandHandler
    public Object handle(EmitEventThatFailsWhenSent command, Order state) {
      return EventThatFailsWhenSent.builder().id(command.getId()).build();
    }

    @EventSourcingHandler
    public Order handle(OrderPlaced event, Order state) {
      return Order.builder().id(event.getId()).build();
    }

    @EventSourcingHandler
    public Order handle(EventWithoutTopic event, Order state) {
      return state;
    }

    @EventSourcingHandler
    public Order handle(EventThatFailsWhenSent event, Order state) {
      return Order.builder().id(event.getId()).build();
    }
  }

  /** Scenario 4: its own topic, so the other scenarios' applications don't read it. */
  @Topic("orders.copy")
  @Value
  @Builder
  public static class EmitEventWithoutCopy {
    @AggregateId
    String id;
  }

  /** Can be written as JSON once: the copy read back has no {@code note}, and {@code getNoteLength()} then fails. */
  @Value
  @Builder
  public static class EventWithoutCopy implements OrderEvent {
    @AggregateId
    String id;
    @JsonIgnore
    String note;

    public int getNoteLength() {
      return note.length();
    }
  }

  public static class CopyHandler {
    @CommandHandler
    public Object handle(EmitEventWithoutCopy command, Order state) {
      return EventWithoutCopy.builder().id(command.getId()).note("not in the JSON").build();
    }

    @EventSourcingHandler
    public Order handle(EventWithoutCopy event, Order state) {
      return Order.builder().id(event.getId()).build();
    }
  }

  /** Scenario 5: can be written as JSON once, as {@link EventWithoutCopy}; every result holds the command. */
  @Topic("orders.command-copy")
  @Value
  @Builder
  public static class CommandWithoutCopy {
    @AggregateId
    String id;
    @JsonIgnore
    String note;

    public int getNoteLength() {
      return note.length();
    }
  }

  /** Scenario 5: a valid command, after the one that fails. */
  @Topic("orders.command-copy")
  @Value
  @Builder
  public static class PlaceOrder {
    @AggregateId
    String id;
  }

  public static class PlaceOrderHandler {
    @CommandHandler
    public Object handle(CommandWithoutCopy command, Order state) {
      return OrderPlaced.builder().id(command.getId()).build();
    }

    @CommandHandler
    public Object handle(PlaceOrder command, Order state) {
      return OrderPlaced.builder().id(command.getId()).build();
    }

    @EventSourcingHandler
    public Order handle(OrderPlaced event, Order state) {
      return Order.builder().id(event.getId()).build();
    }
  }

  /** Scenario 6: its own topic, so it doesn't read the command of scenario 5. */
  @Topic("orders.reply-to")
  @Value
  @Builder
  public static class ShipOrder {
    @AggregateId
    String id;
  }

  public static class ShipOrderHandler {
    @CommandHandler
    public Object handle(ShipOrder command, Order state) {
      return OrderPlaced.builder().id(command.getId()).build();
    }

    @EventSourcingHandler
    public Order handle(OrderPlaced event, Order state) {
      return Order.builder().id(event.getId()).build();
    }
  }

  /** Scenarios 7 and 8: its own topics and aggregate. */
  @AggregateRoot("archive")
  @EnableSnapshotting(threshold = 1)
  @Value
  @Builder
  public static class Archive {
    @AggregateId
    String id;
    String content;
  }

  /** Returns a {@link Filled} with this much content. */
  @Topic("archives")
  @Value
  @Builder
  public static class Fill {
    @AggregateId
    String id;
    int size;
  }

  /** Returns a small {@link Grown}; the archive after it has this much content. */
  @Topic("archives")
  @Value
  @Builder
  public static class Grow {
    @AggregateId
    String id;
    int size;
  }

  /** Scenario 9: a command with this note; returns a small {@link Grown}. */
  @Topic("archives")
  @Value
  @Builder
  public static class Annotate {
    @AggregateId
    String id;
    String note;
  }

  @Topic("archives.events")
  @Value
  @Builder
  public static class Filled {
    @AggregateId
    String id;
    String content;
  }

  @Topic("archives.events")
  @Value
  @Builder
  public static class Grown {
    @AggregateId
    String id;
    int size;
  }

  public static class ArchiveHandler {
    @CommandHandler
    public Object handle(Fill command, Archive state) {
      return Filled.builder().id(command.getId()).content("x".repeat(command.getSize())).build();
    }

    @CommandHandler
    public Object handle(Grow command, Archive state) {
      return Grown.builder().id(command.getId()).size(command.getSize()).build();
    }

    @CommandHandler
    public Object handle(Annotate command, Archive state) {
      return Grown.builder().id(command.getId()).size(1).build();
    }

    @EventSourcingHandler
    public Archive handle(Filled event, Archive state) {
      return Archive.builder().id(event.getId()).content(event.getContent()).build();
    }

    @EventSourcingHandler
    public Archive handle(Grown event, Archive state) {
      return Archive.builder().id(event.getId()).content("x".repeat(event.getSize())).build();
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
          new NewTopic("orders.scheduled", 1, (short) 1),
          new NewTopic("orders.results", 1, (short) 1),
          new NewTopic("orders.events", 1, (short) 1),
          new NewTopic("orders.copy", 1, (short) 1),
          new NewTopic("orders.copy.scheduled", 1, (short) 1),
          new NewTopic("orders.copy.results", 1, (short) 1),
          new NewTopic("orders.command-copy", 1, (short) 1),
          new NewTopic("orders.command-copy.scheduled", 1, (short) 1),
          new NewTopic("orders.command-copy.results", 1, (short) 1),
          new NewTopic("orders.reply-to", 1, (short) 1),
          new NewTopic("orders.reply-to.scheduled", 1, (short) 1),
          new NewTopic("orders.reply-to.results", 1, (short) 1),
          new NewTopic("archives", 1, (short) 1),
          new NewTopic("archives.scheduled", 1, (short) 1),
          new NewTopic("archives.results", 1, (short) 1),
          new NewTopic("archives.events", 1, (short) 1))).all().get();
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

  /** Scenario 4: the event's JSON copy can't be written again. The command fails; Eventify keeps running. */
  @Test
  @DisplayName("Should fail the command, and keep running, when the JSON copy of its event can't be written")
  void aCommandWithAnEventWhoseCopyCantBeWrittenFails() {
    eventify = start("orders-copy-not-writable", new CopyHandler());
    send(EmitEventWithoutCopy.builder().id("order-4").build());
    await("the command to fail or Eventify to stop", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.ERROR
        || !committed("orders.copy.results", "order-4").isEmpty());

    assertThat(eventify.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.RUNNING);
    assertThat(committed("orders.copy.results", "order-4")).as("committed results").containsExactly("failure");
    assertThat(committed("orders.events", "order-4")).as("committed events").isEmpty();
  }

  /** Scenario 5: the command's JSON copy can't be written again, so it can't get a result. It is skipped; Eventify keeps running. */
  @Test
  @DisplayName("Should skip a command that can't be written again, and keep running")
  void aCommandThatCantBeWrittenAgainIsSkipped() {
    eventify = start("orders-command-not-writable", new PlaceOrderHandler());
    send(CommandWithoutCopy.builder().id("order-5").note("not in the JSON").build());
    send(PlaceOrder.builder().id("order-5b").build());
    await("the next command or Eventify to stop", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.ERROR
        || !committed("orders.command-copy.results", "order-5b").isEmpty());

    assertThat(eventify.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.RUNNING);
    assertThat(committed("orders.command-copy.results", "order-5")).as("results of the skipped command").isEmpty();
    assertThat(committed("orders.command-copy.results", "order-5b")).as("results of the next command").containsExactly("success");
  }

  /** Scenario 6: a reply-to header that is no valid topic name. No reply is sent; Eventify keeps running. */
  @Test
  @DisplayName("Should send no reply to an invalid reply topic, and keep running")
  void anInvalidReplyTopicIsSkipped() {
    eventify = start("orders-invalid-reply-to", new ShipOrderHandler());
    Command command = Command.builder().payload(ShipOrder.builder().id("order-6").build()).build();
    try (KafkaProducer<String, Command> producer = new KafkaProducer<>(
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringSerializer(), new JsonSerializer<>())) {
      producer.send(new ProducerRecord<>("orders.reply-to", null, command.getAggregateId(), command,
          List.of(new RecordHeader(HeaderNames.REPLY_TO, "not a topic!".getBytes(StandardCharsets.UTF_8)))));
    }
    send(ShipOrder.builder().id("order-6b").build());
    await("the next command or Eventify to stop", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.ERROR
        || !committed("orders.reply-to.results", "order-6b").isEmpty());

    assertThat(eventify.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.RUNNING);
    assertThat(committed("orders.reply-to.results", "order-6")).as("results of the command").containsExactly("success");
    assertThat(committed("orders.reply-to.results", "order-6b")).as("results of the next command").containsExactly("success");
  }

  /** Scenario 7: an event larger than Kafka accepts (max.request.size, 1 MB by default). The command fails; Eventify keeps running. */
  @Test
  @DisplayName("Should fail the command, and keep running, when its event is too large for Kafka")
  void aCommandWithAnEventTooLargeForKafkaFails() {
    eventify = start("archives-large-event", new ArchiveHandler());
    send(Fill.builder().id("archive-7").size(2_000_000).build());
    send(Fill.builder().id("archive-7b").size(10).build());
    await("the next command or Eventify to stop", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.ERROR
        || !committed("archives.results", "archive-7b").isEmpty());

    assertThat(eventify.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.RUNNING);
    assertThat(committed("archives.results", "archive-7")).as("results of the command").containsExactly("failure");
    assertThat(committed("archives.events", "archive-7")).as("committed events").isEmpty();
    assertThat(committed("archives.results", "archive-7b")).as("results of the next command").containsExactly("success");
  }

  /** Scenario 8: the aggregate has grown larger than Kafka accepts. Its snapshot is skipped; its commands are still accepted. */
  @Test
  @DisplayName("Should skip a snapshot that is too large for Kafka, and keep running")
  void aSnapshotTooLargeForKafkaIsSkipped() {
    eventify = start("archives-large-snapshot", new ArchiveHandler());
    send(Grow.builder().id("archive-8").size(2_000_000).build());
    send(Grow.builder().id("archive-8").size(2_000_001).build());
    await("both commands or Eventify to stop", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.ERROR
        || committed("archives.results", "archive-8").size() == 2);

    assertThat(eventify.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.RUNNING);
    assertThat(committed("archives.results", "archive-8")).as("results of the commands").containsExactly("success", "success");
  }

  /** Scenario 9: the command itself is larger than Eventify's producer accepts, so its failure is too. It gets no result; Eventify keeps running. */
  @Test
  @DisplayName("Should skip a command that is itself too large for Kafka, and keep running")
  void aCommandTooLargeForKafkaGetsNoResult() {
    eventify = start("archives-large-command", new ArchiveHandler());
    // Its sender accepts more than Eventify's producer does; compressed, so the broker accepts it too.
    Command command = Command.builder().payload(Annotate.builder().id("archive-9").note("x".repeat(2_000_000)).build()).build();
    try (KafkaProducer<String, Command> producer = new KafkaProducer<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
        ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 3_000_000,
        ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"), new StringSerializer(), new JsonSerializer<>())) {
      producer.send(new ProducerRecord<>(command.getTopic().value(), command.getAggregateId(), command));
    }
    send(Annotate.builder().id("archive-9b").note("small").build());
    await("the next command or Eventify to stop", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.ERROR
        || !committed("archives.results", "archive-9b").isEmpty());

    assertThat(eventify.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.RUNNING);
    assertThat(committed("archives.results", "archive-9")).as("results of the skipped command").isEmpty();
    assertThat(committed("archives.events", "archive-9")).as("committed events").isEmpty();
    assertThat(committed("archives.results", "archive-9b")).as("results of the next command").containsExactly("success");
  }

  private Eventify start(String applicationId) {
    return start(applicationId, new OrderHandler());
  }

  private Eventify start(String applicationId, Object handler) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    // A stopped instance doesn't leave the group: the broker waits this long before reassigning its partitions.
    properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 6000);
    Eventify instance = Eventify.builder().streamsConfig(properties).registerHandler(handler).build();
    instance.start();
    await("Eventify to run", () -> instance.getKafkaStreams().state() == KafkaStreams.State.RUNNING);
    return instance;
  }

  private static void send(Object payload) {
    Command command = Command.builder().payload(payload).build();
    try (KafkaProducer<String, Command> producer = new KafkaProducer<>(
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringSerializer(), new JsonSerializer<>())) {
      producer.send(new ProducerRecord<>(command.getTopic().value(), command.getAggregateId(), command));
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
