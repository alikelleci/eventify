package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandResult;
import io.github.alikelleci.eventify.core.messaging.commandhandling.gateway.CommandGateway;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** The gateway keeps receiving replies after records on its reply topic it can't read. */
@Testcontainers
@DisplayName("Command gateway replies (real broker)")
class CommandGatewayReplyIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final String COMMAND_TOPIC = "gateway.commands";
  private static final String REPLY_TOPIC = "gateway.replies";

  @TopicInfo(COMMAND_TOPIC)
  @Value
  @Builder
  public static class Ping {
    @AggregateId
    String id;
  }

  @Test
  @DisplayName("Should complete a command after unreadable and empty records on the reply topic")
  void repliesArriveAfterRecordsTheGatewayCannotRead() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(COMMAND_TOPIC, 1, (short) 1), new NewTopic(REPLY_TOPIC, 1, (short) 1))).all().get();
    }

    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    CommandGateway gateway = CommandGateway.builder().producerConfig(producerConfig).replyTopic(REPLY_TOPIC).build();

    Command command = Command.builder().payload(Ping.builder().id("ping-1").build()).build();
    CompletableFuture<Object> future = gateway.send(command);
    // The reply as Eventify writes it: the command itself, with its result.
    byte[] reply = new JsonSerializer<Command>().serialize(REPLY_TOPIC, CommandResult.Success.builder().command(command).build().getCommand());

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
