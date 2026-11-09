package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;

import java.util.List;

/**
 * Shows in Spring Boot's health endpoint whether Eventify still runs: after an error that stops Kafka Streams, the
 * application itself keeps running. Needs Spring Boot Actuator.
 */
@AutoConfiguration(after = EventifyAutoConfiguration.class)
@ConditionalOnClass(HealthIndicator.class)
public class EventifyHealthAutoConfiguration {

  /** Down when Kafka Streams of an Eventify bean has stopped; its state is shown per {@code application.id}. */
  @Bean
  @ConditionalOnBean(Eventify.class)
  @ConditionalOnMissingBean(name = "eventifyHealthIndicator")
  public HealthIndicator eventifyHealthIndicator(List<Eventify> apps) {
    return () -> {
      Health.Builder health = Health.up();
      for (Eventify app : apps) {
        KafkaStreams kafkaStreams = app.getKafkaStreams();
        // Not started: an Eventify bean without handlers has nothing to run.
        if (kafkaStreams == null) {
          continue;
        }
        KafkaStreams.State state = kafkaStreams.state();
        if (!state.isRunningOrRebalancing() && state != KafkaStreams.State.CREATED) {
          health.down();
        }
        health.withDetail(app.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_ID_CONFIG, "eventify"), state.name());
      }
      return health.build();
    };
  }
}
