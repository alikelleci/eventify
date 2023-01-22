package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.util.HandlerUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;

public class EventifyBeanPostProcessor implements BeanPostProcessor {

  private final ApplicationContext applicationContext;

  public EventifyBeanPostProcessor(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  @Override
  public Object postProcessBeforeInitialization(final Object bean, final String beanName) {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(final Object bean, final String beanName) {
    applicationContext.getBeansOfType(Eventify.class)
        .values()
        .forEach(eventify -> HandlerUtils.registerHandler(eventify, bean));

    return bean;
  }
}
