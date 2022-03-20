package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.common.annotations.HandleMessage;
import io.github.alikelleci.eventify.util.HandlerUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.lang.reflect.Method;
import java.util.List;

public class EventifyBeanPostProcessor implements BeanPostProcessor {

  private final EventifyBuilder builder;

  public EventifyBeanPostProcessor(EventifyBuilder eventifyBuilder) {
    this.builder = eventifyBuilder;
  }

  @Override
  public Object postProcessBeforeInitialization(final Object bean, final String beanName) {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(final Object bean, final String beanName) {
    List<Method> methods = HandlerUtils.findMethodsWithAnnotation(bean.getClass(), HandleMessage.class);
    if (CollectionUtils.isNotEmpty(methods)) {
      builder.registerHandler(bean);
    }
    return bean;
  }

}
