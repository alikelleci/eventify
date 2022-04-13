package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.util.HandlerUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;

public class EventifyBeanPostProcessor implements BeanPostProcessor {

  private final Eventify eventify;

  public EventifyBeanPostProcessor(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public Object postProcessBeforeInitialization(final Object bean, final String beanName) {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(final Object bean, final String beanName) {
    HandlerUtils.registerHandler(eventify, bean);
    return bean;
  }
}
