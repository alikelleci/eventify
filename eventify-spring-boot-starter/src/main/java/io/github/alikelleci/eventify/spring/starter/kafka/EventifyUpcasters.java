package io.github.alikelleci.eventify.spring.starter.kafka;

import io.github.alikelleci.eventify.core.handler.internal.HandlerRegistry;
import io.github.alikelleci.eventify.core.upcasting.internal.UpcasterMethod;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.springframework.beans.factory.config.BeanPostProcessor;

/**
 * The {@code @Upcast} methods of all beans, for the events read by {@code @KafkaListener} methods. Collected here,
 * not taken from an Eventify bean: an application that only has listeners needs no Eventify bean.
 *
 * <p>Complete once every singleton exists, before the listener containers start and read the first event.
 */
public class EventifyUpcasters implements BeanPostProcessor {

  private final MultiValuedMap<String, UpcasterMethod> upcasters = new ArrayListValuedHashMap<>();

  @Override
  public Object postProcessAfterInitialization(Object bean, String beanName) {
    HandlerRegistry.registerUpcasters(upcasters, bean);
    return bean;
  }

  public MultiValuedMap<String, UpcasterMethod> getUpcasters() {
    return upcasters;
  }
}
