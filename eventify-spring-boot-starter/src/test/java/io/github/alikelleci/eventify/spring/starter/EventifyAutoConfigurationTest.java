package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.core.messaging.eventhandling.annotations.HandleEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Handler beans with advice (here {@code @Async}; {@code @Transactional} works the same) are registered as the proxy,
 * so the advice applies when a handler runs: registered automatically, and registered explicitly on an Eventify bean.
 */
@ExtendWith(OutputCaptureExtension.class)
@DisplayName("Spring Boot auto-configuration")
class EventifyAutoConfigurationTest {

  /** Without a topic: Eventify has nothing to consume and doesn't connect to Kafka when the context starts. */
  public static class Pinged {
  }

  public static class PingHandler {
    @HandleEvent
    public void on(Pinged event) {
    }

    @Async
    public void later() {
    }
  }

  @Configuration
  @EnableAsync
  static class AutomaticRegistration {
    @Bean
    PingHandler pingHandler() {
      return new PingHandler();
    }

    @Bean
    Eventify eventify() {
      return Eventify.builder().streamsConfig(streamsConfig()).build();
    }
  }

  @Configuration
  @EnableAsync
  static class ExplicitRegistration {
    @Bean
    PingHandler pingHandler() {
      return new PingHandler();
    }

    @Bean
    Eventify eventify(PingHandler pingHandler) {
      return Eventify.builder().streamsConfig(streamsConfig()).registerHandler(pingHandler).build();
    }
  }

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(EventifyAutoConfiguration.class));

  @Test
  @DisplayName("Should register a proxied handler bean automatically, as the proxy")
  void aProxiedHandlerIsRegisteredAutomatically() {
    runner.withUserConfiguration(AutomaticRegistration.class).run(context -> {
      Object handler = registeredHandler(context.getBean(Eventify.class));
      assertThat(AopUtils.isAopProxy(handler)).isTrue();
      assertThat(handler).isSameAs(context.getBean(PingHandler.class));
    });
  }

  @Test
  @DisplayName("Should keep the proxy of a handler bean registered explicitly on an Eventify bean")
  void anExplicitlyRegisteredHandlerKeepsItsProxy(CapturedOutput output) {
    runner.withUserConfiguration(ExplicitRegistration.class).run(context -> {
      Object handler = registeredHandler(context.getBean(Eventify.class));
      assertThat(AopUtils.isAopProxy(handler)).isTrue();
      assertThat(handler).isSameAs(context.getBean(PingHandler.class));
      assertThat(context.getBean(Eventify.class).getEventHandlers().get(Pinged.class)).hasSize(1);
    });
    assertThat(output).doesNotContain("is not eligible for getting processed by all BeanPostProcessors");
  }

  private static Object registeredHandler(Eventify eventify) {
    return eventify.getEventHandlers().get(Pinged.class).stream().map(EventHandler::getHandler).findFirst().orElseThrow();
  }

  private static Properties streamsConfig() {
    Properties properties = new Properties();
    properties.put("application.id", "starter-test");
    properties.put("bootstrap.servers", "localhost:9092");
    return properties;
  }
}
