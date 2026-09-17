package io.github.alikelleci.eventify.core.messaging.commandhandling.gateway;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** A command without a reply times out. */
@Testcontainers
@DisplayName("Command gateway timeout (real broker)")
class CommandGatewayTimingIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final String COMMAND_TOPIC = "timing.commands";
  private static final String REPLY_TOPIC = "timing.replies";

  @BeforeAll
  static void createTopics() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(COMMAND_TOPIC, 1, (short) 1), new NewTopic(REPLY_TOPIC, 1, (short) 1))).all().get();
    }
  }

  @TopicInfo(COMMAND_TOPIC)
  @Value
  @Builder
  public static class Ping {
    @AggregateId
    String id;
  }

  /** Nothing else happens on the gateway after the command: no other command, no reply. */
  @Test
  @DisplayName("Should fail a command without a reply after the timeout, also when nothing else is sent")
  void aCommandWithoutAReplyTimesOut() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    Properties consumerConfig = new Properties();
    consumerConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    DefaultCommandGateway gateway = new DefaultCommandGateway(producerConfig, consumerConfig, REPLY_TOPIC,
        JacksonUtils.enhancedObjectMapper(), Duration.ofSeconds(2));

    CompletableFuture<Object> future = gateway.send(Command.builder().payload(Ping.builder().id("ping-1").build()).build());

    assertThat(future)
        .failsWithin(Duration.ofSeconds(15))
        .withThrowableOfType(ExecutionException.class)
        .withCauseInstanceOf(TimeoutException.class);
  }
}
