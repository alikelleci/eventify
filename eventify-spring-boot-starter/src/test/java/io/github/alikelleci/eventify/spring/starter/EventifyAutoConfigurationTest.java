package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.command.gateway.CommandGateway;
import io.github.alikelleci.eventify.core.event.annotation.HandleEvent;
import io.github.alikelleci.eventify.core.event.internal.EventHandlerMethod;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
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
 * so the advice applies when a handler runs: through the builder bean, and registered explicitly on an Eventify bean.
 */
@ExtendWith(OutputCaptureExtension.class)
@DisplayName("Spring Boot auto-configuration")
class EventifyAutoConfigurationTest {

  @Topic("events.ping")
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
    Eventify eventify(Eventify.EventifyBuilder builder) {
      return builder.streamsConfig(streamsConfig()).build();
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
  @DisplayName("Should register a proxied handler bean on the builder bean, as the proxy")
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
      assertThat(context.getBean(Eventify.class).getHandlers().eventHandlers(Pinged.class)).hasSize(1);
    });
    assertThat(output).doesNotContain("is not eligible for getting processed by all BeanPostProcessors");
  }

  @Configuration
  static class WithoutBuilderBean {
    @Bean
    PingHandler pingHandler() {
      return new PingHandler();
    }

    @Bean
    Eventify eventify() {
      return Eventify.builder().streamsConfig(streamsConfig()).build();
    }
  }

  /** Only the builder bean holds the handler beans: an Eventify built with its own builder is left as it is. */
  @Test
  @DisplayName("Should not register handler beans on an Eventify bean built without the builder bean")
  void anEventifyBuiltWithoutTheBuilderBeanGetsNoHandlerBeans() {
    runner.withUserConfiguration(WithoutBuilderBean.class).run(context ->
        assertThat(context.getBean(Eventify.class).getHandlers().eventHandlers(Pinged.class)).isEmpty());
  }

  @AggregateRoot("counter")
  public static class Counter {
    @AggregateId
    String id;
  }

  @Topic("commands.counter")
  public static class Increment {
    @AggregateId
    String id;
  }

  public static class CounterHandler {
    @HandleCommand
    public Object handle(Increment command, Counter state) {
      return null;
    }
  }

  @Configuration
  static class TwoEventifyBeansFromTheBuilderBean {
    @Bean
    CounterHandler counterHandler() {
      return new CounterHandler();
    }

    @Bean
    Eventify first(Eventify.EventifyBuilder builder) {
      return builder.streamsConfig(streamsConfig()).build();
    }

    @Bean
    Eventify second(Eventify.EventifyBuilder builder) {
      return builder.streamsConfig(streamsConfig()).build();
    }
  }

  @Test
  @DisplayName("Should refuse to start when two Eventify beans handle the same command")
  void aCommandIsHandledByOneEventifyBean() {
    runner.withUserConfiguration(TwoEventifyBeansFromTheBuilderBean.class).run(context ->
        assertThat(context).hasFailed()
            .getFailure().rootCause().hasMessageContaining(Increment.class.getName() + " is handled by more than one Eventify bean"));
  }

  @Test
  @DisplayName("Should give every injection point a builder of its own")
  void theBuildersAreNotShared() {
    runner.run(context -> {
      assertThat(context.getBean(Eventify.EventifyBuilder.class)).isNotSameAs(context.getBean(Eventify.EventifyBuilder.class));
      assertThat(context.getBean(CommandGateway.CommandGatewayBuilder.class)).isNotSameAs(context.getBean(CommandGateway.CommandGatewayBuilder.class));
    });
  }

  private static Object registeredHandler(Eventify eventify) {
    return eventify.getHandlers().eventHandlers(Pinged.class).stream().map(EventHandlerMethod::getHandler).findFirst().orElseThrow();
  }

  private static Properties streamsConfig() {
    Properties properties = new Properties();
    properties.put("application.id", "starter-test");
    properties.put("bootstrap.servers", "localhost:9092");
    return properties;
  }
}
