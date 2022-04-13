package io.github.alikelleci.eventify.util;

import io.github.alikelleci.eventify.Eventify;
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
import lombok.experimental.UtilityClass;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class HandlerUtils {

  public void registerHandler(Eventify eventify, Object handler) {
    List<Method> upcasterMethods = findMethodsWithAnnotation(handler.getClass(), Upcast.class);
    List<Method> commandHandlerMethods = findMethodsWithAnnotation(handler.getClass(), HandleCommand.class);
    List<Method> eventSourcingMethods = findMethodsWithAnnotation(handler.getClass(), ApplyEvent.class);
    List<Method> resultHandlerMethods = findMethodsWithAnnotation(handler.getClass(), HandleResult.class);
    List<Method> eventHandlerMethods = findMethodsWithAnnotation(handler.getClass(), HandleEvent.class);

    upcasterMethods
        .forEach(method -> addUpcaster(eventify, handler, method));

    commandHandlerMethods
        .forEach(method -> addCommandHandler(eventify, handler, method));

    eventSourcingMethods
        .forEach(method -> addEventSourcingHandler(eventify, handler, method));

    resultHandlerMethods
        .forEach(method -> addResultHandler(eventify, handler, method));

    eventHandlerMethods
        .forEach(method -> addEventHandler(eventify, handler, method));
  }

  private  <A extends Annotation> List<Method> findMethodsWithAnnotation(Class<?> c, Class<A> annotation) {
    List<Method> methods = new ArrayList<>();
    for (Method method : c.getDeclaredMethods()) {
      if (AnnotationUtils.findAnnotation(method, annotation) != null) {
        methods.add(method);
      }
    }
    return methods;
  }

  private void addUpcaster(Eventify eventify, Object listener, Method method) {
    if (method.getParameterCount() == 1) {
      String type = method.getAnnotation(Upcast.class).type();
      eventify.getUpcasters().put(type, new Upcaster(listener, method));
    }
  }

  private void addCommandHandler(Eventify eventify, Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      eventify.getCommandHandlers().put(type, new CommandHandler(listener, method));
    }
  }

  private void addEventSourcingHandler(Eventify eventify, Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      eventify.getEventSourcingHandlers().put(type, new EventSourcingHandler(listener, method));
    }
  }

  private void addResultHandler(Eventify eventify, Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      eventify.getResultHandlers().put(type, new ResultHandler(listener, method));
    }
  }

  private void addEventHandler(Eventify eventify, Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      eventify.getEventHandlers().put(type, new EventHandler(listener, method));
    }
  }
}
