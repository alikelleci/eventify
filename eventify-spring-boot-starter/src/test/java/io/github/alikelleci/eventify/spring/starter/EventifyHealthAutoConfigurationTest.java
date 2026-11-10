package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** When the health indicator is there; that it goes down is tested on a real broker, in EventifyHealthIT. */
@DisplayName("Health indicator auto-configuration")
class EventifyHealthAutoConfigurationTest {

  @Configuration
  static class WithEventifyBean {
    @Bean
    Eventify eventify() {
      Properties properties = new Properties();
      properties.put("application.id", "health-test");
      properties.put("bootstrap.servers", "localhost:9092");
      return Eventify.builder().streamsConfig(properties).build();
    }
  }

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(EventifyAutoConfiguration.class, EventifyHealthAutoConfiguration.class));

  @Test
  @DisplayName("Should be up for an Eventify bean without handlers, which doesn't start")
  void upWithoutHandlers() {
    runner.withUserConfiguration(WithEventifyBean.class).run(context ->
        assertThat(context.getBean("eventifyHealthIndicator", HealthIndicator.class).health().getStatus()).isEqualTo(Status.UP));
  }

  @Test
  @DisplayName("Should add no health indicator without an Eventify bean")
  void noneWithoutEventify() {
    runner.run(context -> assertThat(context).doesNotHaveBean("eventifyHealthIndicator"));
  }

  @Test
  @DisplayName("Should add no health indicator without Spring Boot's health support")
  void noneWithoutActuator() {
    runner.withUserConfiguration(WithEventifyBean.class)
        .withClassLoader(new FilteredClassLoader(HealthIndicator.class))
        .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean("eventifyHealthIndicator"));
  }
}
