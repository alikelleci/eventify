package io.github.alikelleci.eventify.core.util;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.core.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.core.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.core.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.core.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.core.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.eventify.core.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.core.messaging.upcasting.annotations.Upcast;
import lombok.experimental.UtilityClass;
import org.apache.commons.collections4.MultiValuedMap;

import java.lang.reflect.Method;
import java.util.Arrays;

@UtilityClass
public class HandlerUtils {

  public void registerHandler(Eventify eventify, Object handler) {
    AnnotationUtils.findAnnotatedMethods(handler.getClass(), HandleCommand.class)
        .forEach(method -> addCommandHandler(eventify, handler, method));

    AnnotationUtils.findAnnotatedMethods(handler.getClass(), ApplyEvent.class)
        .forEach(method -> addEventSourcingHandler(eventify, handler, method));

    AnnotationUtils.findAnnotatedMethods(handler.getClass(), HandleResult.class)
        .forEach(method -> addResultHandler(eventify, handler, method));

    AnnotationUtils.findAnnotatedMethods(handler.getClass(), HandleEvent.class)
        .forEach(method -> addEventHandler(eventify, handler, method));

    registerUpcasters(eventify.getUpcasters(), handler);
  }

  /**
   * Adds the {@link Upcast} methods of a handler to the upcasters. Also for upcasters outside Eventify, e.g. a
   * {@code JsonSerde} that reads the events for a projection.
   */
  public void registerUpcasters(MultiValuedMap<String, Upcaster> upcasters, Object handler) {
    AnnotationUtils.findAnnotatedMethods(handler.getClass(), Upcast.class)
        .forEach(method -> addUpcaster(upcasters, handler, method));
  }


  private void addCommandHandler(Eventify eventify, Object listener, Method method) {
    if (method.getParameterCount() >= 1) {
      Class<?> type = method.getParameters()[0].getType();
      CommandHandler previous = eventify.getCommandHandlers().put(type, new CommandHandler(listener, method));
      if (previous != null) {
        requireSameHandler("@HandleCommand", type, previous.getHandler(), previous.getMethod(), listener, method);
      }
    }
  }

  private void addEventSourcingHandler(Eventify eventify, Object listener, Method method) {
    if (method.getParameterCount() >= 1) {
      Class<?> type = method.getParameters()[0].getType();
      EventSourcingHandler previous = eventify.getEventSourcingHandlers().put(type, new EventSourcingHandler(listener, method));
      if (previous != null) {
        requireSameHandler("@ApplyEvent", type, previous.getHandler(), previous.getMethod(), listener, method);
      }
    }
  }

  /**
   * Throws when the class already has a @HandleCommand or @ApplyEvent handler.
   * Allowed: registering the same handler object twice, and a subclass method that overrides an annotated method.
   */
  private void requireSameHandler(String annotation, Class<?> type, Object previousHandler, Method previousMethod, Object handler, Method method) {
    boolean sameHandler = previousHandler == handler
        && previousMethod.getName().equals(method.getName())
        && Arrays.equals(previousMethod.getParameterTypes(), method.getParameterTypes());
    if (!sameHandler) {
      throw new IllegalStateException("Two " + annotation + " handlers for " + type.getName() + ": " + previousMethod + " and " + method);
    }
  }

  private void addResultHandler(Eventify eventify, Object listener, Method method) {
    if (method.getParameterCount() >= 1) {
      Class<?> type = method.getParameters()[0].getType();
      eventify.getResultHandlers().put(type, new ResultHandler(listener, method));
    }
  }

  private void addEventHandler(Eventify eventify, Object listener, Method method) {
    if (method.getParameterCount() >= 1) {
      Class<?> type = method.getParameters()[0].getType();
      eventify.getEventHandlers().put(type, new EventHandler(listener, method));
    }
  }

  /** One upcaster per type and revision: with two, the chain would take one of them, depending on the order they were registered in. */
  private void addUpcaster(MultiValuedMap<String, Upcaster> upcasters, Object listener, Method method) {
    if (method.getParameterCount() == 1) {
      Upcast upcast = method.getAnnotation(Upcast.class);
      upcasters.get(upcast.type()).stream()
          .filter(existing -> existing.getMethod().getAnnotation(Upcast.class).revision() == upcast.revision())
          .findFirst()
          .ifPresent(existing -> {
            throw new IllegalStateException("Two upcasters for " + upcast.type() + " revision " + upcast.revision() + ": " + existing.getMethod() + " and " + method);
          });
      upcasters.put(upcast.type(), new Upcaster(listener, method));
    }
  }
}
