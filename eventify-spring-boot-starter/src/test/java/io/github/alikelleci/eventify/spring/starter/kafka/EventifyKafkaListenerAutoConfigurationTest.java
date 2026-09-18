package io.github.alikelleci.eventify.spring.starter.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.config.KafkaListenerContainerFactory;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("@KafkaListener auto-configuration")
class EventifyKafkaListenerAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(EventifyKafkaListenerAutoConfiguration.class));

  @Test
  @DisplayName("Should do nothing without spring-kafka")
  void nothingWithoutSpringKafka() {
    runner.withClassLoader(new FilteredClassLoader(KafkaListenerContainerFactory.class)).run(context -> {
      assertThat(context).hasNotFailed();
      assertThat(context).doesNotHaveBean(EventifyUpcasters.class);
    });
  }

  /** Nothing to connect to: the application reads Kafka in another way, or not at all. */
  @Test
  @DisplayName("Should not define the container factory without an Eventify bean or a consumer factory")
  void noContainerFactoryWithoutAConnection() {
    runner.run(context -> {
      assertThat(context).hasNotFailed();
      assertThat(context).doesNotHaveBean("eventifyListenerContainerFactory");
    });
  }
}
