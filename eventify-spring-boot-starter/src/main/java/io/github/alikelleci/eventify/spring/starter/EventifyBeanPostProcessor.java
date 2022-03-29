package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.common.annotations.HandleMessage;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.util.HandlerUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.lang.reflect.Method;
import java.util.List;

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
    List<Method> methods = HandlerUtils.findMethodsWithAnnotation(bean.getClass(), HandleMessage.class);
    if (CollectionUtils.isNotEmpty(methods)) {
      registerHandler(bean);
    }
    return bean;
  }

  private void registerHandler(Object handler) {
    List<Method> upcasterMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), Upcast.class);
    List<Method> commandHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleCommand.class);
    List<Method> eventSourcingMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), ApplyEvent.class);
    List<Method> resultHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleResult.class);
    List<Method> eventHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleEvent.class);

    upcasterMethods
        .forEach(method -> addUpcaster(handler, method));

    commandHandlerMethods
        .forEach(method -> addCommandHandler(handler, method));

    eventSourcingMethods
        .forEach(method -> addEventSourcingHandler(handler, method));

    resultHandlerMethods
        .forEach(method -> addResultHandler(handler, method));

    eventHandlerMethods
        .forEach(method -> addEventHandler(handler, method));
  }

  private void addUpcaster(Object listener, Method method) {
    if (method.getParameterCount() == 1) {
      String type = method.getAnnotation(Upcast.class).type();
      eventify.getUpcasters().put(type, new Upcaster(listener, method));
    }
  }

  private void addCommandHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      eventify.getCommandHandlers().put(type, new CommandHandler(listener, method));
    }
  }

  private void addEventSourcingHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      eventify.getEventSourcingHandlers().put(type, new EventSourcingHandler(listener, method));
    }
  }

  private void addResultHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      eventify.getResultHandlers().put(type, new ResultHandler(listener, method));
    }
  }

  private void addEventHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      eventify.getEventHandlers().put(type, new EventHandler(listener, method));
    }
  }
}
