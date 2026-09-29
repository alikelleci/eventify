package io.github.alikelleci.eventify.migration;

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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** The check on changelogs written the way Eventify 4.0.3 wrote them, against a real broker. */
@Testcontainers
@DisplayName("Migration check (Eventify 4 changelogs, real broker)")
class MigrationCheckIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final String ORDER_PLACED = "com.example.order.OrderPlaced";
  private static final String ORDER_SHIPPED = "com.example.order.OrderShipped";
  private static final AtomicLong CLOCK = new AtomicLong(Instant.parse("2025-03-01T10:00:00Z").toEpochMilli());

  @Test
  @DisplayName("Should count the live events of an Eventify 4 store and report no conflicts")
  void aCleanStore() throws Exception {
    String app = "clean";
    createChangelogs(app);
    String first = writeEvent(app, 0, "order-1", ORDER_PLACED);
    write(app + "-event-store-changelog", 0, first, v4Event(first, "order-1", ORDER_PLACED)); // the older value stays until compaction
    writeEvent(app, 0, "order-1", ORDER_SHIPPED);
    String deleted = writeEvent(app, 0, "order-1", ORDER_SHIPPED);
    write(app + "-event-store-changelog", 0, deleted, null);
    writeEvent(app, 1, "order-2", ORDER_PLACED);

    CheckReport report = check(app, false);

    assertThat(report.conflicts).isEmpty();
    assertThat(report.eventPartitions).isEqualTo(2);
    assertThat(report.events).isEqualTo(3);
    assertThat(report.aggregates).isEqualTo(2);
    assertThat(report.largestAggregate).isEqualTo("order-1");
    assertThat(report.largestAggregateEvents).isEqualTo(2);
    assertThat(report.eventClasses).containsExactly(Map.entry(ORDER_PLACED, 2L), Map.entry(ORDER_SHIPPED, 1L));
    assertThat(report.v4Snapshots).isZero();
    assertThat(report.sharedRanges).isEmpty();
  }

  @Test
  @DisplayName("Should report an aggregate whose Eventify 4 range also held another aggregate's events, in the same partition only")
  void sharedRanges() throws Exception {
    String app = "shared";
    createChangelogs(app);
    writeEvent(app, 0, "ada", ORDER_PLACED);
    writeEvent(app, 0, "ada@example.com", ORDER_PLACED);
    writeEvent(app, 0, "ada@example.com", ORDER_SHIPPED);
    writeEvent(app, 1, "bob", ORDER_PLACED);
    writeEvent(app, 0, "bob@example.com", ORDER_PLACED);

    CheckReport report = check(app, false);

    assertThat(report.conflicts).isEmpty();
    assertThat(report.sharedRanges).containsExactly(new CheckReport.SharedRange("ada", "ada@example.com", 2));
  }

  @Test
  @DisplayName("Should report every record the migration cannot handle, and write nothing")
  void conflicts() throws Exception {
    String app = "conflicts";
    createChangelogs(app);
    write(app + "-event-store-changelog", 0, "not-a-v4-key", "{}");
    String mismatch = v4Key("order-1");
    write(app + "-event-store-changelog", 0, mismatch, v4Event(mismatch, "order-other", ORDER_PLACED));
    write(app + "-event-store-changelog", 0, v4Key("order-2"), "not json");
    writeEvent(app, 0, "order-3", ORDER_PLACED);
    writeEvent(app, 1, "order-3", ORDER_PLACED);
    write(app + "-event-store-changelog", 1, "order\u0000order-4\u00000000000000000000001", "{}");
    write(app + "-snapshot-store-changelog", 0, "order-3", "{}");
    long endOffsets = endOffsets(app + "-event-store-changelog") + endOffsets(app + "-snapshot-store-changelog");

    CheckReport report = check(app, false);

    assertThat(report.conflicts).hasSize(6).satisfiesExactlyInAnyOrder(
        conflict -> assertThat(conflict).startsWith("Not an Eventify 4 event key: not-a-v4-key"),
        conflict -> assertThat(conflict).contains("has aggregateId \"order-other\""),
        conflict -> assertThat(conflict).contains("is not JSON"),
        conflict -> assertThat(conflict).isEqualTo("Aggregate order-3 has events in partitions 0 and 1."),
        conflict -> assertThat(conflict).isEqualTo("Event order␀order-4␀0000000000000000001 has no payload with an @class."),
        conflict -> assertThat(conflict).startsWith("1 Eventify 4 snapshots cannot be migrated."));
    assertThat(endOffsets(app + "-event-store-changelog") + endOffsets(app + "-snapshot-store-changelog")).isEqualTo(endOffsets);
  }

  @Test
  @DisplayName("Should accept Eventify 4 snapshots when they may be dropped")
  void droppedSnapshots() throws Exception {
    String app = "snapshots";
    createChangelogs(app);
    writeEvent(app, 0, "order-1", ORDER_PLACED);
    write(app + "-snapshot-store-changelog", 0, "order-1", "{}");

    CheckReport report = check(app, true);

    assertThat(report.conflicts).isEmpty();
    assertThat(report.v4Snapshots).isEqualTo(1);
  }

  @Test
  @DisplayName("Should report a wrong application id instead of an empty store")
  void missingTopic() {
    CheckReport report = check("no-such-app", false);

    assertThat(report.conflicts).containsExactly("Topic no-such-app-event-store-changelog does not exist. Is the application id right?");
  }

  @Test
  @DisplayName("Should exit with 1 on conflicts and 2 on wrong arguments")
  void exitCodes() {
    assertThat(MigrationTool.run(new String[]{"check", "--bootstrap-servers", kafka.getBootstrapServers(),
        "--application-id", "no-such-app", "--aggregate-type", "order"})).isEqualTo(1);
    assertThat(MigrationTool.run(new String[]{"check", "--application-id", "app"})).isEqualTo(2);
    assertThat(MigrationTool.run(new String[]{"rollback"})).isEqualTo(2);
  }

  private static CheckReport check(String app, boolean dropSnapshots) {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    CheckReport report = new MigrationCheck(config, "order", dropSnapshots).run(app);
    report.print(System.out, "Check");
    return report;
  }

  private static void createChangelogs(String app) throws Exception {
    Map<String, String> compacted = Map.of("cleanup.policy", "compact");
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(
          new NewTopic(app + "-event-store-changelog", 2, (short) 1).configs(compacted),
          new NewTopic(app + "-snapshot-store-changelog", 2, (short) 1).configs(compacted))).all().get();
    }
  }

  private static void write(String topic, int partition, String key, String value) throws Exception {
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringSerializer(), new StringSerializer())) {
      producer.send(new ProducerRecord<>(topic, partition, key, value)).get();
    }
  }

  private static long endOffsets(String topic) {
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
        Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringDeserializer(), new StringDeserializer())) {
      List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
          .map(info -> new TopicPartition(topic, info.partition())).toList();
      return consumer.endOffsets(partitions).values().stream().mapToLong(Long::longValue).sum();
    }
  }

  /** Stores a new Eventify 4 event and returns its key. */
  private static String writeEvent(String app, int partition, String aggregateId, String payloadClass) throws Exception {
    String key = v4Key(aggregateId);
    write(app + "-event-store-changelog", partition, key, v4Event(key, aggregateId, payloadClass));
    return key;
  }

  /** An Eventify 4 key: the aggregate id, "@" and a ULID of a later millisecond than the previous key. */
  private static String v4Key(String aggregateId) {
    return aggregateId + "@" + ulid(CLOCK.addAndGet(1));
  }

  /** An event as Eventify 4.0.3 stored it: no aggregateType, no sequence, and its key as its id. */
  private static String v4Event(String id, String aggregateId, String payloadClass) {
    return """
        {"aggregateId":"%s","id":"%s","metadata":{"$correlationId":"c-%s"},"payload":{"@class":"%s","id":"%s"},\
        "revision":1,"timestamp":"2025-03-01T10:00:00Z","type":"%s"}"""
        .formatted(aggregateId, id, aggregateId, payloadClass, aggregateId,
            payloadClass.substring(payloadClass.lastIndexOf('.') + 1));
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
}
