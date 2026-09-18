package io.github.alikelleci.eventify.spring.starter.kafka;

import io.github.alikelleci.eventify.core.upcasting.Upcasters;
import org.springframework.beans.factory.config.BeanPostProcessor;

/**
 * The {@code @Upcast} methods of all beans, for the events read by {@code @KafkaListener} methods. Collected here,
 * not taken from an Eventify bean: an application that only has listeners needs no Eventify bean.
 *
 * <p>Complete once every singleton exists, before the listener containers start and read the first event.
 */
public class EventifyUpcasters implements BeanPostProcessor {

  private final Upcasters upcasters = new Upcasters();

  @Override
  public Object postProcessAfterInitialization(Object bean, String beanName) {
    upcasters.register(bean);
    return bean;
  }

  public Upcasters getUpcasters() {
    return upcasters;
  }
}
