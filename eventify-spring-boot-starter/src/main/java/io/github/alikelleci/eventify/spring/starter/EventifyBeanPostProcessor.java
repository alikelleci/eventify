package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.util.HandlerUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.util.List;
import java.util.stream.Collectors;

public class EventifyBeanPostProcessor implements BeanPostProcessor {

  private final List<Eventify> apps;

  public EventifyBeanPostProcessor(List<Eventify> apps) {
    this.apps = apps.stream()
        .filter(eventify -> eventify.getUpcasters().isEmpty())
        .filter(eventify -> eventify.getCommandHandlers().isEmpty())
        .filter(eventify -> eventify.getEventSourcingHandlers().isEmpty())
        .filter(eventify -> eventify.getResultHandlers().isEmpty())
        .filter(eventify -> eventify.getEventHandlers().isEmpty())
        .collect(Collectors.toList());
  }

  @Override
  public Object postProcessBeforeInitialization(final Object bean, final String beanName) {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(final Object bean, final String beanName) {
    apps.forEach(eventify ->
        HandlerUtils.registerHandler(eventify, bean));

    return bean;
  }
}
