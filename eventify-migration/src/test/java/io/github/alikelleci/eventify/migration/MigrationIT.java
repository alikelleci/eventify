package io.github.alikelleci.eventify.migration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Migrates changelogs written the way Eventify 4.0.3 wrote them, then runs Eventify 5 on them, against a real broker. */
@Testcontainers
@DisplayName("Migration (Eventify 4 changelogs to Eventify 5, real broker)")
class MigrationIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final int PARTITIONS = 2;
  private static final AtomicLong CLOCK = new AtomicLong(Instant.parse("2025-03-01T10:00:00Z").toEpochMilli());
  /** Numbers exactly as written, so the test itself does not round the decimals it checks. */
  private static final ObjectMapper JSON = new ObjectMapper()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

  @AggregateRoot("order")
  @Value
  @Builder(toBuilder = true)
  public static class Order {
    @AggregateId
    String id;
    int items;
  }

  @Topic("orders")
  @Value
  @Builder
  public static class AddItem {
    @AggregateId
    String id;
  }

  /** Carries the item count it was added to, so a replay that misses or mixes events shows in the next one. */
  @Topic("orders.events")
  @Value
  @Builder
  public static class ItemAdded {
    @AggregateId
    String id;
    int itemsBefore;
    BigDecimal price;
  }

  public static class OrderHandler {
    @CommandHandler
    public ItemAdded handle(AddItem command, Order state) {
      return ItemAdded.builder().id(command.getId()).itemsBefore(state == null ? 0 : state.getItems()).price(BigDecimal.ONE).build();
    }

    @EventSourcingHandler
    public Order handle(ItemAdded event, Order state) {
      return Order.builder().id(event.getId()).items(state == null ? 1 : state.getItems() + 1).build();
    }
  }

  @TempDir
  Path stateDir;

  private Eventify eventify;

  @BeforeAll
  static void createTopics() throws Exception {
    try (AdminClient admin = admin()) {
      admin.createTopics(List.of(
          new NewTopic("orders", PARTITIONS, (short) 1),
          new NewTopic("orders.results", PARTITIONS, (short) 1),
          new NewTopic("orders.events", PARTITIONS, (short) 1))).all().get();
    }
  }

  @AfterEach
  void tearDown() {
    if (eventify != null) eventify.stop();
  }

  @Test
  @DisplayName("Should number each aggregate's events 1..n, after which Eventify 5 replays them and goes on at n + 1")
  void eventify5GoesOnWhereEventify4Stopped() throws Exception {
    String app = "shop";
    createChangelogs(app);
    for (int i = 0; i < 3; i++) {
      writeV4Event(app, "order-1", i, "10.50");
    }
    writeV4Event(app, "ada", 0, "1");
    writeV4Event(app, "ada@example.com", 0, "2");
    writeV4Event(app, "ada@example.com", 1, "0.1000000000000000055511151231257827");

    assertThat(migrate(app)).isZero();

    Map<String, JsonNode> events = liveEvents(app);
    assertThat(events.keySet()).containsExactlyInAnyOrder(
        Keys.v5Event("order", "order-1", 1), Keys.v5Event("order", "order-1", 2), Keys.v5Event("order", "order-1", 3),
        Keys.v5Event("order", "ada", 1),
        Keys.v5Event("order", "ada@example.com", 1), Keys.v5Event("order", "ada@example.com", 2));
    JsonNode third = events.get(Keys.v5Event("order", "order-1", 3));
    assertThat(third.get("aggregateType").asText()).isEqualTo("order");
    assertThat(third.get("sequence").asLong()).isEqualTo(3);
    assertThat(third.get("id").asText()).startsWith("order-1@");
    assertThat(third.get("payload").get("itemsBefore").asInt()).isEqualTo(2);
    assertThat(rawValue(app, Keys.v5Event("order", "order-1", 3))).contains("\"price\":10.50");
    assertThat(rawValue(app, Keys.v5Event("order", "ada@example.com", 2))).contains("\"price\":0.1000000000000000055511151231257827");

    eventify = start(app);
    send(AddItem.builder().id("order-1").build());
    send(AddItem.builder().id("ada").build());
    await("the new events", () -> liveEvents(app).size() == 8);

    assertThat(liveEvents(app).get(Keys.v5Event("order", "order-1", 4)).get("payload").get("itemsBefore").asInt()).isEqualTo(3);
    // In Eventify 4 "ada" also replayed the events of "ada@example.com"; now only its own.
    assertThat(liveEvents(app).get(Keys.v5Event("order", "ada", 2)).get("payload").get("itemsBefore").asInt()).isEqualTo(1);
  }

  @Test
  @DisplayName("Should finish an interrupted migration and do nothing on a migrated store")
  void resumes() throws Exception {
    String app = "resumed";
    createChangelogs(app);
    String first = writeV4Event(app, "order-1", 0, "1");
    writeV4Event(app, "order-1", 1, "1");
    writeV4Event(app, "order-1", 2, "1");
    // What an interrupted run leaves: the first event under its new key, its tombstone committed with it.
    ObjectNode firstEvent = (ObjectNode) v4Event(first, "order-1", 0, "1");
    firstEvent.put("aggregateType", "order").put("sequence", 1);
    write(app + "-event-store-changelog", partitionOf("order-1"), Keys.v5Event("order", "order-1", 1), JSON.writeValueAsString(firstEvent));
    write(app + "-event-store-changelog", partitionOf("order-1"), first, null);

    assertThat(migrate(app)).isZero();
    assertThat(liveEvents(app)).containsOnlyKeys(
        Keys.v5Event("order", "order-1", 1), Keys.v5Event("order", "order-1", 2), Keys.v5Event("order", "order-1", 3));
    assertThat(liveEvents(app).get(Keys.v5Event("order", "order-1", 1)).get("id").asText()).isEqualTo(first);

    long records = endOffset(app + "-event-store-changelog");
    assertThat(migrate(app)).isZero();
    assertThat(endOffset(app + "-event-store-changelog")).as("nothing written the second time").isEqualTo(records);
  }

  @Test
  @DisplayName("Should refuse an interrupted migration whose written event does not have the sequence of its place")
  void refusesAMisplacedMigratedEvent() throws Exception {
    String app = "misplaced";
    createChangelogs(app);
    writeV4Event(app, "order-1", 0, "1");
    String second = writeV4Event(app, "order-1", 1, "1");
    ObjectNode secondEvent = (ObjectNode) v4Event(second, "order-1", 1, "1");
    secondEvent.put("aggregateType", "order").put("sequence", 1);
    write(app + "-event-store-changelog", partitionOf("order-1"), Keys.v5Event("order", "order-1", 1), JSON.writeValueAsString(secondEvent));
    write(app + "-event-store-changelog", partitionOf("order-1"), second, null);
    long records = endOffset(app + "-event-store-changelog");

    assertThat(migrate(app)).isEqualTo(1);
    assertThat(endOffset(app + "-event-store-changelog")).as("nothing written").isEqualTo(records);
  }

  @Test
  @DisplayName("Should refuse to write while the application still runs")
  void refusesWhileTheApplicationRuns() throws Exception {
    String app = "running";
    createChangelogs(app);
    writeV4Event(app, "order-1", 0, "1");
    long records = endOffset(app + "-event-store-changelog");

    try (KafkaConsumer<String, String> member = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG, app), new StringDeserializer(), new StringDeserializer())) {
      member.subscribe(List.of("orders"));
      await("the member to join", () -> {
        member.poll(Duration.ofMillis(200));
        return !member.assignment().isEmpty();
      });

      assertThat(migrate(app)).isEqualTo(1);
    }
    assertThat(endOffset(app + "-event-store-changelog")).as("nothing written").isEqualTo(records);
  }

  @Test
  @DisplayName("Should delete Eventify 4 snapshots with --drop-snapshots, and refuse them without")
  void snapshots() throws Exception {
    String app = "snapshotted";
    createChangelogs(app);
    writeV4Event(app, "order-1", 0, "1");
    write(app + "-snapshot-store-changelog", partitionOf("order-1"), "order-1", "{}");

    assertThat(MigrationTool.run(args("migrate", app))).isEqualTo(1);
    assertThat(MigrationTool.run(args("migrate", app, "--drop-snapshots"))).isZero();
    assertThat(MigrationTool.run(args("verify", app))).isZero();
  }

  private static int migrate(String app) {
    return MigrationTool.run(args("migrate", app));
  }

  private static String[] args(String command, String app, String... more) {
    List<String> args = new ArrayList<>(List.of(command, "--bootstrap-servers", kafka.getBootstrapServers(),
        "--application-id", app, "--aggregate-type", "order"));
    args.addAll(List.of(more));
    return args.toArray(String[]::new);
  }

  private Eventify start(String app) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, app);
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

  /** The store's live events by key, as a read_committed restore would see them. */
  private static Map<String, JsonNode> liveEvents(String app) {
    Map<String, JsonNode> events = new TreeMap<>();
    readLive(app + "-event-store-changelog", (key, value) -> {
      try {
        events.put(key, JSON.readTree(value));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
    return events;
  }

  private static String rawValue(String app, String key) {
    StringBuilder value = new StringBuilder();
    readLive(app + "-event-store-changelog", (k, v) -> {
      if (k.equals(key)) value.append(new String(v, StandardCharsets.UTF_8));
    });
    return value.toString();
  }

  private static void readLive(String topic, BiConsumer<String, byte[]> action) {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    try (ChangelogReader reader = new ChangelogReader(config)) {
      for (int number : reader.partitions(topic)) {
        TopicPartition partition = new TopicPartition(topic, number);
        long end = reader.end(partition);
        reader.forEachLive(partition, end, reader.liveOffsets(partition, end), action);
      }
    }
  }

  private static long endOffset(String topic) {
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
        Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringDeserializer(), new StringDeserializer())) {
      List<TopicPartition> partitions = consumer.partitionsFor(topic).stream().map(info -> new TopicPartition(topic, info.partition())).toList();
      return consumer.endOffsets(partitions).values().stream().mapToLong(Long::longValue).sum();
    }
  }

  private static void createChangelogs(String app) throws Exception {
    Map<String, String> compacted = Map.of("cleanup.policy", "compact");
    try (AdminClient admin = admin()) {
      admin.createTopics(List.of(
          new NewTopic(app + "-event-store-changelog", PARTITIONS, (short) 1).configs(compacted),
          new NewTopic(app + "-snapshot-store-changelog", PARTITIONS, (short) 1).configs(compacted))).all().get();
    }
  }

  /** Stores an event as Eventify 4.0.3 did, in the partition of the aggregate's commands, and returns its key. */
  private static String writeV4Event(String app, String aggregateId, int itemsBefore, String price) throws Exception {
    String key = aggregateId + "@" + ulid(CLOCK.addAndGet(1));
    write(app + "-event-store-changelog", partitionOf(aggregateId), key, JSON.writeValueAsString(v4Event(key, aggregateId, itemsBefore, price)));
    return key;
  }

  /** No aggregateType, no sequence, and its key as its id. */
  private static JsonNode v4Event(String key, String aggregateId, int itemsBefore, String price) throws Exception {
    return JSON.readTree("""
        {"aggregateId":"%s","id":"%s","metadata":{"$correlationId":"c-1"},\
        "payload":{"@class":"%s","id":"%s","itemsBefore":%d,"price":%s},\
        "revision":1,"timestamp":"2025-03-01T10:00:00Z","type":"ItemAdded"}"""
        .formatted(aggregateId, key, ItemAdded.class.getName(), aggregateId, itemsBefore, price));
  }

  /** Where Kafka's default partitioner puts the aggregate's commands, and so its store. */
  private static int partitionOf(String aggregateId) {
    return Utils.toPositive(Utils.murmur2(aggregateId.getBytes(StandardCharsets.UTF_8))) % PARTITIONS;
  }

  private static void write(String topic, int partition, String key, String value) throws Exception {
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringSerializer(), new StringSerializer())) {
      producer.send(new ProducerRecord<>(topic, partition, key, value)).get();
    }
  }

  private static AdminClient admin() {
    return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
  }

  /** 10 characters of time and 16 of randomness, in Crockford base32, like UlidCreator. */
  private static String ulid(long millis) {
    String alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    StringBuilder ulid = new StringBuilder();
    for (int i = 9; i >= 0; i--) {
      ulid.append(alphabet.charAt((int) (millis >>> (i * 5)) & 31));
    }
    for (int i = 0; i < 16; i++) {
      ulid.append(alphabet.charAt((int) (Math.random() * 32)));
    }
    return ulid.toString();
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
