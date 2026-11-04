package io.github.alikelleci.eventify.core.command.gateway;

import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.TopicNames;
import io.github.alikelleci.eventify.core.command.internal.HeaderNames;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
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
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@DisplayName("Command gateway scheduling (real broker)")
class CommandGatewayScheduleIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final String COMMAND_TOPIC = "gateway.schedule.commands";
  private static final String SCHEDULED_TOPIC = TopicNames.scheduledTopicOf(COMMAND_TOPIC);
  private static final String REPLY_TOPIC = "gateway.schedule.replies";

  @Topic(COMMAND_TOPIC)
  @Value
  @Builder
  public static class Ping {
    @AggregateId
    String id;
  }

  @Topic(COMMAND_TOPIC)
  @Value
  @Builder
  public static class Upload {
    @AggregateId
    String id;
    String content;
  }

  private CommandGateway gateway;

  @BeforeAll
  static void createTopics() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(
          new NewTopic(COMMAND_TOPIC, 1, (short) 1),
          new NewTopic(SCHEDULED_TOPIC, 1, (short) 1),
          new NewTopic(REPLY_TOPIC, 1, (short) 1))).all().get();
    }
  }

  @AfterEach
  void tearDown() {
    if (gateway != null) gateway.close();
  }

  @Test
  @DisplayName("Should complete once Kafka has the command, written to the scheduled topic with its time and no reply topic")
  void writesTheCommandToTheScheduledTopic() {
    gateway = gateway();
    Command command = Command.builder().payload(Ping.builder().id("ping-1").build()).build();
    Instant deliverAt = Instant.parse("2030-01-01T00:00:00Z");

    CompletableFuture<Void> future = gateway.schedule(command, deliverAt);

    assertThat(future).succeedsWithin(Duration.ofSeconds(10));
    List<ConsumerRecord<String, String>> records = read(SCHEDULED_TOPIC);
    assertThat(records).hasSize(1);
    ConsumerRecord<String, String> record = records.get(0);
    assertThat(record.key()).isEqualTo("ping-1");
    assertThat(record.value()).contains(command.getId());
    assertThat(record.timestamp()).isEqualTo(command.getTimestamp().toEpochMilli());
    assertThat(new String(record.headers().lastHeader(HeaderNames.DELIVER_AT).value(), StandardCharsets.UTF_8))
        .isEqualTo(Long.toString(deliverAt.toEpochMilli()));
    assertThat(record.headers().lastHeader(HeaderNames.REPLY_TO)).isNull();
    assertThat(read(COMMAND_TOPIC)).as("sent to the command topic right away").isEmpty();
  }

  @Test
  @DisplayName("Should fail the future with the reason when the command cannot be sent")
  void failsWithTheReason() {
    gateway = gateway();
    // Larger than the producer sends (max.request.size, 1 MB by default).
    Upload upload = Upload.builder().id("upload-1").content("x".repeat(2 * 1024 * 1024)).build();

    assertThat(gateway.schedule(upload, Instant.now()))
        .failsWithin(Duration.ofSeconds(30))
        .withThrowableOfType(ExecutionException.class)
        .withCauseInstanceOf(RecordTooLargeException.class);
  }

  @Test
  @DisplayName("Should refuse a missing or pre-1970 time, and scheduling after close")
  void refusesInvalidInput() {
    gateway = gateway();
    Ping ping = Ping.builder().id("ping-2").build();

    assertThatThrownBy(() -> gateway.schedule(ping, null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> gateway.schedule(ping, Instant.ofEpochMilli(-1))).isInstanceOf(IllegalArgumentException.class);

    gateway.close();
    assertThatThrownBy(() -> gateway.schedule(ping, Instant.now())).isInstanceOf(IllegalStateException.class);
  }

  private static CommandGateway gateway() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    return CommandGateway.builder().producerConfig(producerConfig).replyTopic(REPLY_TOPIC).build();
  }

  private static List<ConsumerRecord<String, String>> read(String topic) {
    List<ConsumerRecord<String, String>> found = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new StringDeserializer())) {
      TopicPartition partition = new TopicPartition(topic, 0);
      consumer.assign(List.of(partition));
      consumer.seekToBeginning(List.of(partition));
      long end = consumer.endOffsets(List.of(partition)).get(partition);
      while (consumer.position(partition) < end) {
        consumer.poll(Duration.ofMillis(500)).forEach(found::add);
      }
    }
    return found;
  }
}
