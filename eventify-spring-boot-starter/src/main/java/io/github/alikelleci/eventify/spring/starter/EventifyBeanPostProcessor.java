package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.common.annotations.HandleMessage;
import io.github.alikelleci.eventify.core.util.AnnotationUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ClassUtils;

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
      apps.forEach(eventify -> eventify.registerHandler(bean));
    }
    return bean;
  }

  /**
   * Looks at the bean's own class and its superclasses. A bean with advice (e.g. {@code @Transactional}) is a CGLIB
   * subclass whose methods don't carry the annotations; registering the bean itself still invokes the handlers through
   * that advice.
   */
  private boolean isHandler(Object bean) {
    return !AnnotationUtils.findAnnotatedMethods(ClassUtils.getUserClass(bean), HandleMessage.class).isEmpty();
  }
}
