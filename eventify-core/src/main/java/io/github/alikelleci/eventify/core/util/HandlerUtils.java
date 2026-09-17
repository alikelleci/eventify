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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  /**
   * The handler for a message of this class: the one registered for the class itself, otherwise for the nearest
   * superclass, otherwise for the first interface found, nearest first. A handler may be written for a supertype, e.g.
   * {@code handle(OrderCommand command)}; the message always has its own class.
   *
   * @return {@code null} when neither the class nor any of its supertypes has a handler
   */
  public <H> H findHandler(Map<Class<?>, H> handlers, Class<?> type) {
    for (Class<?> candidate : typeHierarchy(type)) {
      H handler = handlers.get(candidate);
      if (handler != null) {
        return handler;
      }
    }
    return null;
  }

  /** The handlers for a message of this class: those of the class itself and of all its supertypes. */
  public <H> List<H> findHandlers(MultiValuedMap<Class<?>, H> handlers, Class<?> type) {
    List<H> found = new ArrayList<>();
    for (Class<?> candidate : typeHierarchy(type)) {
      found.addAll(handlers.get(candidate));
    }
    return found;
  }

  /** The class, its superclasses, then their interfaces (breadth first), each once; nearest first. */
  private List<Class<?>> typeHierarchy(Class<?> type) {
    Set<Class<?>> types = new LinkedHashSet<>();
    for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
      types.add(current);
    }
    Deque<Class<?>> interfaces = new ArrayDeque<>();
    new ArrayList<>(types).forEach(current -> interfaces.addAll(Arrays.asList(current.getInterfaces())));
    while (!interfaces.isEmpty()) {
      Class<?> current = interfaces.poll();
      if (types.add(current)) {
        interfaces.addAll(Arrays.asList(current.getInterfaces()));
      }
    }
    return new ArrayList<>(types);
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
