package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;

import java.util.List;

@Slf4j
@AutoConfiguration
@ConditionalOnBean(Eventify.class)
@EnableConfigurationProperties(EventifyProperties.class)
public class EventifyAutoConfiguration {

  @Bean
  public EventifyBeanPostProcessor eventifyBeanPostProcessor(List<Eventify> apps) {
    return new EventifyBeanPostProcessor(apps);
  }

  @Bean
  @ConditionalOnClass(name = "org.springframework.web.client.RestClient")
  public EventifyQueryController eventifyQueryController(Eventify eventify) {
    return new EventifyQueryController(eventify);
  }

  @Bean
  public SmartLifecycle eventifyLifecycle(List<Eventify> apps) {
    return new SmartLifecycle() {
      private volatile boolean running = false;

      @Override
      public void start() {
        apps.forEach(Eventify::start);
        running = true;
      }

      @Override
      public void stop() {
        apps.forEach(Eventify::stop);
        running = false;
      }

      @Override
      public boolean isRunning() {
        return running;
      }

      @Override
      public int getPhase() {
        return Integer.MAX_VALUE;
      }
    };
  }
}
