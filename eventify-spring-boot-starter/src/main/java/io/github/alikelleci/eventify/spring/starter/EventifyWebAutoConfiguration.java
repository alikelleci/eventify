package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.web.EventifyWebPlugin;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;

import java.util.List;

@AutoConfiguration
@ConditionalOnClass(EventifyWebPlugin.class)
@ConditionalOnBean(Eventify.class)
public class EventifyWebAutoConfiguration {

  @Bean
  public InitializingBean eventifyWebPluginRegistrar(List<Eventify> apps) {
    return () -> apps.forEach(eventify -> eventify.registerPlugin(new EventifyWebPlugin()));
  }
}
