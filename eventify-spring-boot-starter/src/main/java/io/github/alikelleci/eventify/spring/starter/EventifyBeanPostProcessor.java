package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.common.annotations.HandleMessage;
import io.github.alikelleci.eventify.core.util.AnnotationUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Registers the handler beans on the Eventify beans that were built without handlers. An Eventify bean with handlers
 * registered explicitly gets none: with several Eventify beans, each one registers its own.
 *
 * <p>The Eventify beans are only looked up once every singleton exists. Asked for when this post-processor is created,
 * they and everything they depend on (explicitly registered handlers too) would be created before the other
 * post-processors are registered, and miss them.
 */
public class EventifyBeanPostProcessor implements BeanPostProcessor, SmartInitializingSingleton {

  private final ObjectProvider<Eventify> apps;
  private final List<Object> handlers = new ArrayList<>();

  public EventifyBeanPostProcessor(ObjectProvider<Eventify> apps) {
    this.apps = apps;
  }

  @Override
  public Object postProcessAfterInitialization(final Object bean, final String beanName) {
    if (isHandler(bean)) {
      handlers.add(bean);
    }
    return bean;
  }

  @Override
  public void afterSingletonsInstantiated() {
    List<Eventify> withoutHandlers = apps.orderedStream()
        .filter(eventify -> eventify.getCommandHandlers().isEmpty())
        .filter(eventify -> eventify.getEventSourcingHandlers().isEmpty())
        .filter(eventify -> eventify.getResultHandlers().isEmpty())
        .filter(eventify -> eventify.getEventHandlers().isEmpty())
        .filter(eventify -> eventify.getUpcasters().isEmpty())
        .toList();

    // With more than one, every handler would be registered on each of them, and every command handled as many times.
    // Which handler belongs to which application is the application's own choice: it registers them itself.
    if (withoutHandlers.size() > 1) {
      throw new IllegalStateException("There is more than one Eventify bean without handlers (" + withoutHandlers.size()
          + "). Register the handlers on each Eventify bean yourself, with Eventify.builder().registerHandler(...), "
          + "so each one handles what it should.");
    }

    handlers.forEach(handler -> withoutHandlers.forEach(eventify -> eventify.registerHandler(handler)));
    handlers.clear();
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
