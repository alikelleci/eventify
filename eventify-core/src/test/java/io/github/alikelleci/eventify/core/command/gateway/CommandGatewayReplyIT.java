package io.github.alikelleci.eventify.core.command.gateway;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.internal.CommandReplies;
import io.github.alikelleci.eventify.core.command.internal.CommandResult;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The gateway keeps receiving replies after records on its reply topic it can't read. */
@Testcontainers
@DisplayName("Command gateway replies (real broker)")
class CommandGatewayReplyIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final String COMMAND_TOPIC = "gateway.commands";
  private static final String REPLY_TOPIC = "gateway.replies";

  @BeforeAll
  static void createTopics() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(COMMAND_TOPIC, 1, (short) 1), new NewTopic(REPLY_TOPIC, 1, (short) 1))).all().get();
    }
  }

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

  @Test
  @DisplayName("Should fail a command that cannot be sent with the reason, not with a timeout")
  void aCommandThatCannotBeSentFailsWithTheReason() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    CommandGateway gateway = CommandGateway.builder().producerConfig(producerConfig).replyTopic(REPLY_TOPIC).build();

    // Larger than the producer sends (max.request.size, 1 MB by default).
    Upload upload = Upload.builder().id("upload-1").content("x".repeat(2 * 1024 * 1024)).build();
    CompletableFuture<Object> future = gateway.send(Command.builder().payload(upload).build());

    assertThat(future)
        .failsWithin(Duration.ofSeconds(30))
        .withThrowableOfType(ExecutionException.class)
        .withCauseInstanceOf(RecordTooLargeException.class);
  }

  @Test
  @DisplayName("Should refuse the same command while it waits, and fail waiting commands when the gateway closes")
  void theSameCommandIsRefusedWhileItWaitsAndClosingFailsWaitingCommands() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    CommandGateway gateway = CommandGateway.builder().producerConfig(producerConfig).replyTopic(REPLY_TOPIC).build();

    // Nothing handles the command: it waits for its reply.
    Command command = Command.builder().payload(Ping.builder().id("ping-closing").build()).build();
    CompletableFuture<Object> waiting = gateway.send(command);
    CompletableFuture<Object> again = gateway.send(command);

    assertThat(again)
        .failsWithin(Duration.ofSeconds(1))
        .withThrowableOfType(ExecutionException.class)
        .withCauseInstanceOf(IllegalStateException.class);
    assertThat(waiting).isNotDone();

    gateway.close();

    assertThat(waiting)
        .failsWithin(Duration.ofSeconds(1))
        .withThrowableThat()
        .isInstanceOf(CancellationException.class);
    assertThatThrownBy(() -> gateway.send(Command.builder().payload(Ping.builder().id("ping-after-close").build()).build()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  @DisplayName("Should complete a command after unreadable and empty records on the reply topic")
  void repliesArriveAfterRecordsTheGatewayCannotRead() throws Exception {

    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    CommandGateway gateway = CommandGateway.builder().producerConfig(producerConfig).replyTopic(REPLY_TOPIC).build();

    Command command = Command.builder().payload(Ping.builder().id("ping-1").build()).build();
    CompletableFuture<Object> future = gateway.send(command);
    // The reply as Eventify writes it: the command itself, with its result.
    byte[] reply = new JsonSerializer<Command>().serialize(REPLY_TOPIC, CommandReplies.toReply(CommandResult.Success.builder().command(command).build()));

    Properties rawConfig = new Properties();
    rawConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    try (KafkaProducer<String, byte[]> raw = new KafkaProducer<>(rawConfig, new StringSerializer(), new ByteArraySerializer())) {
      // Again until answered: the gateway starts reading at the end of the topic, from whenever it is assigned.
      Instant deadline = Instant.now().plusSeconds(30);
      while (!future.isDone() && Instant.now().isBefore(deadline)) {
        raw.send(new ProducerRecord<>(REPLY_TOPIC, 0, "ping-1", "not json".getBytes(StandardCharsets.UTF_8))).get();
        raw.send(new ProducerRecord<>(REPLY_TOPIC, 0, "ping-1", null)).get();
        raw.send(new ProducerRecord<>(REPLY_TOPIC, 0, "ping-1", reply)).get();
        Thread.sleep(Duration.ofSeconds(1).toMillis());
      }
    }

    assertThat(future).isCompleted();
    assertThat(future.get()).isEqualTo(command.getPayload());
  }
}
