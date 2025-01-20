package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;

import java.util.List;
import java.util.Map;

@Slf4j
@AutoConfiguration
@ConditionalOnBean(Eventify.class)
@EnableConfigurationProperties(EventifyProperties.class)
public class EventifyAutoConfiguration {

  @Autowired
  private ApplicationContext applicationContext;

  @Bean
  public EventifyBeanPostProcessor eventifyBeanPostProcessor(@Autowired List<Eventify> apps) {
    return new EventifyBeanPostProcessor(apps);
  }

  @EventListener
  public void onApplicationEvent(ApplicationReadyEvent event) {
    if (event.getApplicationContext().equals(this.applicationContext)) {
      Map<String, Eventify> apps = event.getApplicationContext().getBeansOfType(Eventify.class);
      apps.values().forEach(Eventify::start);
    }
  }
}
