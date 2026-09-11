package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.console.EventifyConsolePlugin;
import io.github.alikelleci.eventify.core.Eventify;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;

import java.util.List;

@AutoConfiguration
@ConditionalOnClass(EventifyConsolePlugin.class)
@ConditionalOnBean(Eventify.class)
public class EventifyConsoleAutoConfiguration {

  @Bean
  public InitializingBean eventifyConsolePluginRegistrar(List<Eventify> apps) {
    return () -> apps.forEach(eventify -> eventify.registerPlugin(new EventifyConsolePlugin()));
  }
}
