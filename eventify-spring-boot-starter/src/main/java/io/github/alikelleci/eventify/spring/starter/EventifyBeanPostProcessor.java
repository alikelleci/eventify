package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.common.annotations.HandleMessage;
import io.github.alikelleci.eventify.core.util.AnnotationUtils;
import io.github.alikelleci.eventify.core.util.HandlerUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.util.Arrays;
import java.util.List;

public class EventifyBeanPostProcessor implements BeanPostProcessor {

  private final List<Eventify> apps;

  public EventifyBeanPostProcessor(List<Eventify> apps) {
    this.apps = apps.stream()
        .filter(eventify -> eventify.getCommandHandlers().isEmpty())
        .filter(eventify -> eventify.getEventSourcingHandlers().isEmpty())
        .filter(eventify -> eventify.getResultHandlers().isEmpty())
        .filter(eventify -> eventify.getEventHandlers().isEmpty())
        .filter(eventify -> eventify.getUpcasters().isEmpty())
        .toList();
  }

  @Override
  public Object postProcessBeforeInitialization(final Object bean, final String beanName) {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(final Object bean, final String beanName) {
    if (isHandler(bean)) {
      apps.forEach(eventify -> HandlerUtils.registerHandler(eventify, bean));
    }
    return bean;
  }

  private boolean isHandler(Object bean) {
    return Arrays.stream(bean.getClass().getDeclaredMethods())
        .anyMatch(method -> AnnotationUtils.findAnnotation(method, HandleMessage.class) != null);
  }
}
