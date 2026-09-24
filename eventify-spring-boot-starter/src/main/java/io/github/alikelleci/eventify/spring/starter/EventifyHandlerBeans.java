package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.util.ClassUtils;

import java.util.Arrays;
import java.util.List;

/** The singleton beans with Eventify handler or upcaster methods. */
public final class EventifyHandlerBeans {

  private EventifyHandlerBeans() {
  }

  /**
   * Picked by bean type, so only handlers are created. A bean with advice (e.g. {@code @Transactional}) is returned
   * as its proxy, so the advice applies when a handler runs.
   */
  public static List<Object> of(ListableBeanFactory beanFactory) {
    return Arrays.stream(beanFactory.getBeanNamesForType(Object.class, false, false))
        .filter(name -> isHandler(beanFactory.getType(name, false)))
        .map(beanFactory::getBean)
        .toList();
  }

  private static boolean isHandler(Class<?> beanType) {
    return beanType != null && Eventify.isHandler(ClassUtils.getUserClass(beanType));
  }
}
