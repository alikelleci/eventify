package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.AggregateRoot;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
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
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reply topic of a command is named by whoever sent it. One that cannot be written to costs that sender its
 * answer, and nothing else: the command itself is handled, and the instance goes on handling the commands after it.
 */
@Testcontainers
@DisplayName("A reply topic that cannot be written to (real broker)")
class ReplyTopicFailureIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  @AggregateRoot
  @Value
  @Builder
  public static class Order {
    @AggregateId
    String id;
  }

  @TopicInfo("reply-test.orders")
  @Value
  @Builder
  public static class PlaceOrder {
    @AggregateId
    String id;
  }

  @TopicInfo("reply-test.orders.events")
  @Value
  @Builder
  public static class OrderPlaced {
    @AggregateId
    String id;
  }

  public static class OrderHandler {
    @HandleCommand
    public Object handle(PlaceOrder command, Order state) {
      return OrderPlaced.builder().id(command.getId()).build();
    }

    @ApplyEvent
    public Order apply(OrderPlaced event, Order state) {
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
          new NewTopic("reply-test.orders", 1, (short) 1),
          new NewTopic("reply-test.orders.results", 1, (short) 1),
          new NewTopic("reply-test.orders.events", 1, (short) 1))).all().get();
    }
  }

  @AfterEach
  void tearDown() {
    if (eventify != null) eventify.stop();
  }

  @Test
  @DisplayName("Should handle the command and keep running when its result cannot be sent to the reply topic")
  void aReplyThatCannotBeSentDoesNotStopCommandHandling() {
    eventify = start();
    await("Eventify to run", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.RUNNING);

    // A topic name Kafka refuses: the result cannot be sent there, whatever the broker does with unknown topics.
    send("order-1", "not a valid topic name!");
    send("order-2", null);

    // Both commands are handled, and the instance is still running: only the first sender gets no answer.
    await("both commands to be handled", () -> committedResults().containsAll(List.of("order-1", "order-2")));
    assertThat(eventify.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.RUNNING);
    assertThat(committedEvents()).contains("order-1", "order-2");
  }

  private Eventify start() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "reply-failure-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

    Eventify eventify = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new OrderHandler())
        .build();
    eventify.start();
    return eventify;
  }

  private static void send(String id, String replyTopic) {
    Command.CommandBuilder builder = Command.builder().payload(PlaceOrder.builder().id(id).build());
    if (replyTopic != null) {
      builder.metadata(Metadata.REPLY_TO, replyTopic);
    }
    Command command = builder.build();
    try (KafkaProducer<String, Command> producer = new KafkaProducer<>(
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringSerializer(), new JsonSerializer<>())) {
      producer.send(new ProducerRecord<>("reply-test.orders", command.getAggregateId(), command));
    }
  }

  private static List<String> committedResults() {
    return keysOf("reply-test.orders.results");
  }

  private static List<String> committedEvents() {
    return keysOf("reply-test.orders.events");
  }

  /** The record keys of a topic, as a {@code read_committed} consumer sees them: the aggregate ids. */
  private static List<String> keysOf(String topic) {
    List<String> found = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new StringDeserializer())) {
      TopicPartition partition = new TopicPartition(topic, 0);
      consumer.assign(List.of(partition));
      consumer.seekToBeginning(List.of(partition));
      long end = consumer.endOffsets(List.of(partition)).get(partition);
      while (consumer.position(partition) < end) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
          if (record.offset() < end && record.key() != null) {
            found.add(record.key());
          }
        }
      }
    }
    return found;
  }

  private static void await(String what, BooleanSupplier condition) {
    Instant deadline = Instant.now().plusSeconds(120);
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
