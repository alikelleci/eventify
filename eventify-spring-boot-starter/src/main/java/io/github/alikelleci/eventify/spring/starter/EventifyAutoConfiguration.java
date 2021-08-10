package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.EventifyBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

@Slf4j
@Configuration
@ConditionalOnBean(EventifyBuilder.class)
@EnableConfigurationProperties(EventifyProperties.class)
public class EventifyAutoConfiguration {

  @Autowired
  private ApplicationContext applicationContext;

  @Bean
  public EventifyBeanPostProcessor eventifyBeanPostProcessor(EventifyBuilder builder) {
    return new EventifyBeanPostProcessor(builder);
  }

  @EventListener
  public void onApplicationEvent(ApplicationReadyEvent event) {
    if (event.getApplicationContext().equals(this.applicationContext)) {
      EventifyBuilder builder = event.getApplicationContext().getBean(EventifyBuilder.class);
      Eventify app = builder.build();
      app.start();
    }
  }
}
