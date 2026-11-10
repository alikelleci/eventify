package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.annotation.EventHandler;
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
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/** The health indicator of an Eventify bean that runs, and of one an event handler stopped. */
@Testcontainers
@DisplayName("Health indicator (real broker)")
class EventifyHealthIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  @Topic("health.events")
  @Value
  @Builder
  public static class OrderPlaced {
    @AggregateId
    String id;
  }

  public static class FailingEventHandler {
    @EventHandler
    public void handle(OrderPlaced event) {
      throw new IllegalStateException("projection failed");
    }
  }

  @Configuration
  static class WithFailingEventHandler {
    @Bean
    FailingEventHandler failingEventHandler() {
      return new FailingEventHandler();
    }

    @Bean
    Eventify eventify(Eventify.EventifyBuilder builder) {
      Properties properties = new Properties();
      properties.put("application.id", "health-it");
      properties.put("bootstrap.servers", kafka.getBootstrapServers());
      return builder.streamsConfig(properties).build();
    }
  }

  @Test
  @DisplayName("Should be up while Eventify runs, and down once an event handler has stopped it")
  void downAfterEventifyStopped() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic("health.events", 1, (short) 1))).all().get();
    }

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(EventifyAutoConfiguration.class, EventifyHealthAutoConfiguration.class))
        .withUserConfiguration(WithFailingEventHandler.class)
        .run(context -> {
          HealthIndicator indicator = context.getBean("eventifyHealthIndicator", HealthIndicator.class);
          await("Eventify to run", () -> "RUNNING".equals(indicator.health().getDetails().get("health-it")));
          assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);

          Event event = Event.builder().aggregateType("order").payload(OrderPlaced.builder().id("order-1").build()).sequence(1).build();
          try (KafkaProducer<String, Event> producer = new KafkaProducer<>(
              Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringSerializer(), new JsonSerializer<>())) {
            producer.send(new ProducerRecord<>("health.events", event.getAggregateId(), event)).get();
          }

          await("Eventify to stop", () -> "ERROR".equals(indicator.health().getDetails().get("health-it")));
          assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
          // What this is for: the application itself keeps running.
          assertThat(context.isRunning()).isTrue();
        });
  }

  private static void await(String what, BooleanSupplier condition) throws InterruptedException {
    Instant deadline = Instant.now().plusSeconds(90);
    while (!condition.getAsBoolean()) {
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      Thread.sleep(200);
    }
  }
}
