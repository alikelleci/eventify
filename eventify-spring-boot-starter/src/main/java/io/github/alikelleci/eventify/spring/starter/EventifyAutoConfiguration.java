package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@AutoConfiguration
@ConditionalOnBean(Eventify.class)
@EnableConfigurationProperties(EventifyProperties.class)
public class EventifyAutoConfiguration {

  /** Static: a post-processor created from an instance method would first create this configuration, too early. */
  @Bean
  public static EventifyBeanPostProcessor eventifyBeanPostProcessor(ObjectProvider<Eventify> apps) {
    return new EventifyBeanPostProcessor(apps);
  }

  @Bean
  public SmartLifecycle eventifyLifecycle(List<Eventify> apps) {
    return new SmartLifecycle() {
      private volatile boolean running = false;

      /**
       * Starts the apps in order. When one fails to start, the ones started before it are stopped again: Spring doesn't
       * stop a lifecycle that failed to start, so they would keep running outside the failed context.
       */
      @Override
      public void start() {
        List<Eventify> started = new ArrayList<>();
        try {
          for (Eventify app : apps) {
            app.start();
            started.add(app);
          }
        } catch (RuntimeException e) {
          Collections.reverse(started);
          started.forEach(app -> {
            try {
              app.stop();
            } catch (RuntimeException stopFailure) {
              e.addSuppressed(stopFailure);
            }
          });
          throw e;
        }
        running = true;
      }

      /** In reverse order of starting. */
      @Override
      public void stop() {
        List<Eventify> reversed = new ArrayList<>(apps);
        Collections.reverse(reversed);
        reversed.forEach(Eventify::stop);
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
