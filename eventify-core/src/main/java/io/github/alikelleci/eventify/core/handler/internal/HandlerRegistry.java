package io.github.alikelleci.eventify.core.handler.internal;

import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.aggregate.internal.ApplyEventMethod;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.command.internal.CommandHandlerMethod;
import io.github.alikelleci.eventify.core.event.annotation.HandleEvent;
import io.github.alikelleci.eventify.core.event.internal.EventHandlerMethod;
import io.github.alikelleci.eventify.core.handler.annotation.HandleMessage;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcast;
import io.github.alikelleci.eventify.core.upcasting.internal.UpcasterMethod;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The handlers of one Eventify instance: its command handlers, event sourcing handlers, event handlers and upcasters,
 * found on the objects registered with it.
 *
 * <p>Frozen when Eventify starts: from then on the stream threads read it, and a handler added then would be seen by
 * some of them and not by others.
 */
public class HandlerRegistry {

  private final Map<Class<?>, CommandHandlerMethod> commandHandlers = new HashMap<>();
  private final Map<Class<?>, ApplyEventMethod> eventSourcingHandlers = new HashMap<>();
  private final MultiValuedMap<Class<?>, EventHandlerMethod> eventHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<String, UpcasterMethod> upcasters = new ArrayListValuedHashMap<>();

  private volatile boolean frozen;

  /** Whether the class has a method with an Eventify handler annotation, e.g. {@code @HandleCommand} or {@code @Upcast}. */
  public static boolean isHandler(Class<?> type) {
    return !AnnotationScanner.findAnnotatedMethods(type, HandleMessage.class).isEmpty();
  }

  public void register(Object handler) {
    if (frozen) {
      throw new IllegalStateException("Eventify is started: handlers can only be registered before it starts.");
    }
    AnnotationScanner.findAnnotatedMethods(handler.getClass(), HandleCommand.class)
        .forEach(method -> addCommandHandler(handler, method));

    AnnotationScanner.findAnnotatedMethods(handler.getClass(), ApplyEvent.class)
        .forEach(method -> addEventSourcingHandler(handler, method));

    AnnotationScanner.findAnnotatedMethods(handler.getClass(), HandleEvent.class)
        .forEach(method -> addEventHandler(handler, method));

    registerUpcasters(upcasters, handler);
  }

  /**
   * Adds the {@link Upcast} methods of a handler to the upcasters. Also for upcasters outside Eventify, e.g. a
   * {@code JsonSerde} that reads the events for a projection.
   */
  public static void registerUpcasters(MultiValuedMap<String, UpcasterMethod> upcasters, Object handler) {
    AnnotationScanner.findAnnotatedMethods(handler.getClass(), Upcast.class)
        .forEach(method -> addUpcaster(upcasters, handler, method));
  }

  public void freeze() {
    frozen = true;
  }

  public boolean isEmpty() {
    return commandHandlers.isEmpty() && eventSourcingHandlers.isEmpty() && eventHandlers.isEmpty() && upcasters.isEmpty();
  }

  /** The command handler for this command class; {@code null} when there is none. */
  public CommandHandlerMethod commandHandler(Class<?> commandType) {
    return commandHandlers.get(commandType);
  }

  public Map<Class<?>, CommandHandlerMethod> commandHandlers() {
    return Collections.unmodifiableMap(commandHandlers);
  }

  /** The event sourcing handler for this event class; {@code null} when there is none. */
  public ApplyEventMethod eventSourcingHandler(Class<?> eventType) {
    return eventSourcingHandlers.get(eventType);
  }

  public Map<Class<?>, ApplyEventMethod> eventSourcingHandlers() {
    return Collections.unmodifiableMap(eventSourcingHandlers);
  }

  public boolean hasEventHandlers() {
    return !eventHandlers.isEmpty();
  }

  /** The event handlers for this event class, in the order they were registered; empty when there are none. */
  public Collection<EventHandlerMethod> eventHandlers(Class<?> eventType) {
    return Collections.unmodifiableCollection(eventHandlers.get(eventType));
  }

  /**
   * The upcasters, by the class name they upcast. The map itself, not a copy: a serde made with it also upcasts with
   * the upcasters registered after it was made.
   */
  public MultiValuedMap<String, UpcasterMethod> upcasters() {
    return upcasters;
  }

  /** The topics of the commands that have a command handler. */
  public Set<String> commandTopics() {
    return topicsOf(commandHandlers.keySet());
  }

  /** The topics of the events that have an event handler. */
  public Set<String> eventTopics() {
    return topicsOf(eventHandlers.keySet());
  }

  private static Set<String> topicsOf(Set<Class<?>> types) {
    return types.stream()
        .map(aClass -> AnnotationScanner.findAnnotation(aClass, Topic.class))
        .filter(Objects::nonNull)
        .map(Topic::value)
        .collect(Collectors.toSet());
  }

  private void addCommandHandler(Object handler, Method method) {
    if (method.getParameterCount() >= 1) {
      Class<?> type = method.getParameters()[0].getType();
      CommandHandlerMethod previous = commandHandlers.put(type, new CommandHandlerMethod(handler, method));
      if (previous != null) {
        requireSameHandler("@HandleCommand", type, previous.getHandler(), previous.getMethod(), handler, method);
      }
    }
  }

  private void addEventSourcingHandler(Object handler, Method method) {
    if (method.getParameterCount() >= 1) {
      Class<?> type = method.getParameters()[0].getType();
      ApplyEventMethod previous = eventSourcingHandlers.put(type, new ApplyEventMethod(handler, method));
      if (previous != null) {
        requireSameHandler("@ApplyEvent", type, previous.getHandler(), previous.getMethod(), handler, method);
      }
    }
  }

  /**
   * Throws when the class already has a @HandleCommand or @ApplyEvent handler.
   * Allowed: registering the same handler object twice, and a subclass method that overrides an annotated method.
   */
  private static void requireSameHandler(String annotation, Class<?> type, Object previousHandler, Method previousMethod, Object handler, Method method) {
    boolean sameHandler = previousHandler == handler
        && previousMethod.getName().equals(method.getName())
        && Arrays.equals(previousMethod.getParameterTypes(), method.getParameterTypes());
    if (!sameHandler) {
      throw new IllegalStateException("Two " + annotation + " handlers for " + type.getName() + ": " + previousMethod + " and " + method);
    }
  }

  private void addEventHandler(Object handler, Method method) {
    if (method.getParameterCount() >= 1) {
      Class<?> type = method.getParameters()[0].getType();
      eventHandlers.put(type, new EventHandlerMethod(handler, method));
    }
  }

  /** One upcaster per type and revision: with two, the chain would take one of them, depending on the order they were registered in. */
  private static void addUpcaster(MultiValuedMap<String, UpcasterMethod> upcasters, Object handler, Method method) {
    if (method.getParameterCount() == 1) {
      Upcast upcast = method.getAnnotation(Upcast.class);
      upcasters.get(upcast.type()).stream()
          .filter(existing -> existing.getMethod().getAnnotation(Upcast.class).revision() == upcast.revision())
          .findFirst()
          .ifPresent(existing -> {
            throw new IllegalStateException("Two upcasters for " + upcast.type() + " revision " + upcast.revision() + ": " + existing.getMethod() + " and " + method);
          });
      upcasters.put(upcast.type(), new UpcasterMethod(handler, method));
    }
  }
}
