package io.github.alikelleci.eventify.core.handler.internal;

import io.github.alikelleci.eventify.core.common.annotations.HandleMessage;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.core.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.core.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.core.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.core.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.core.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.core.util.AnnotationUtils;
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

  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers = new HashMap<>();
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<String, Upcaster> upcasters = new ArrayListValuedHashMap<>();

  private volatile boolean frozen;

  /** Whether the class has a method with an Eventify handler annotation, e.g. {@code @HandleCommand} or {@code @Upcast}. */
  public static boolean isHandler(Class<?> type) {
    return !AnnotationUtils.findAnnotatedMethods(type, HandleMessage.class).isEmpty();
  }

  public void register(Object handler) {
    if (frozen) {
      throw new IllegalStateException("Eventify is started: handlers can only be registered before it starts.");
    }
    AnnotationUtils.findAnnotatedMethods(handler.getClass(), HandleCommand.class)
        .forEach(method -> addCommandHandler(handler, method));

    AnnotationUtils.findAnnotatedMethods(handler.getClass(), ApplyEvent.class)
        .forEach(method -> addEventSourcingHandler(handler, method));

    AnnotationUtils.findAnnotatedMethods(handler.getClass(), HandleEvent.class)
        .forEach(method -> addEventHandler(handler, method));

    registerUpcasters(upcasters, handler);
  }

  /**
   * Adds the {@link Upcast} methods of a handler to the upcasters. Also for upcasters outside Eventify, e.g. a
   * {@code JsonSerde} that reads the events for a projection.
   */
  public static void registerUpcasters(MultiValuedMap<String, Upcaster> upcasters, Object handler) {
    AnnotationUtils.findAnnotatedMethods(handler.getClass(), Upcast.class)
        .forEach(method -> addUpcaster(upcasters, handler, method));
  }

  public void freeze() {
    frozen = true;
  }

  public boolean isEmpty() {
    return commandHandlers.isEmpty() && eventSourcingHandlers.isEmpty() && eventHandlers.isEmpty() && upcasters.isEmpty();
  }

  /** The command handler for this command class; {@code null} when there is none. */
  public CommandHandler commandHandler(Class<?> commandType) {
    return commandHandlers.get(commandType);
  }

  public Map<Class<?>, CommandHandler> commandHandlers() {
    return Collections.unmodifiableMap(commandHandlers);
  }

  /** The event sourcing handler for this event class; {@code null} when there is none. */
  public EventSourcingHandler eventSourcingHandler(Class<?> eventType) {
    return eventSourcingHandlers.get(eventType);
  }

  public Map<Class<?>, EventSourcingHandler> eventSourcingHandlers() {
    return Collections.unmodifiableMap(eventSourcingHandlers);
  }

  public boolean hasEventHandlers() {
    return !eventHandlers.isEmpty();
  }

  /** The event handlers for this event class, in the order they were registered; empty when there are none. */
  public Collection<EventHandler> eventHandlers(Class<?> eventType) {
    return Collections.unmodifiableCollection(eventHandlers.get(eventType));
  }

  /**
   * The upcasters, by the class name they upcast. The map itself, not a copy: a serde made with it also upcasts with
   * the upcasters registered after it was made.
   */
  public MultiValuedMap<String, Upcaster> upcasters() {
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
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private void addCommandHandler(Object handler, Method method) {
    if (method.getParameterCount() >= 1) {
      Class<?> type = method.getParameters()[0].getType();
      CommandHandler previous = commandHandlers.put(type, new CommandHandler(handler, method));
      if (previous != null) {
        requireSameHandler("@HandleCommand", type, previous.getHandler(), previous.getMethod(), handler, method);
      }
    }
  }

  private void addEventSourcingHandler(Object handler, Method method) {
    if (method.getParameterCount() >= 1) {
      Class<?> type = method.getParameters()[0].getType();
      EventSourcingHandler previous = eventSourcingHandlers.put(type, new EventSourcingHandler(handler, method));
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
      eventHandlers.put(type, new EventHandler(handler, method));
    }
  }

  /** One upcaster per type and revision: with two, the chain would take one of them, depending on the order they were registered in. */
  private static void addUpcaster(MultiValuedMap<String, Upcaster> upcasters, Object handler, Method method) {
    if (method.getParameterCount() == 1) {
      Upcast upcast = method.getAnnotation(Upcast.class);
      upcasters.get(upcast.type()).stream()
          .filter(existing -> existing.getMethod().getAnnotation(Upcast.class).revision() == upcast.revision())
          .findFirst()
          .ifPresent(existing -> {
            throw new IllegalStateException("Two upcasters for " + upcast.type() + " revision " + upcast.revision() + ": " + existing.getMethod() + " and " + method);
          });
      upcasters.put(upcast.type(), new Upcaster(handler, method));
    }
  }
}
