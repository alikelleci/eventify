package io.github.alikelleci.eventify.core.handler.internal;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.aggregate.internal.AggregateTypes;
import io.github.alikelleci.eventify.core.aggregate.internal.ApplyEventMethod;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.command.internal.CommandHandlerMethod;
import io.github.alikelleci.eventify.core.event.annotation.HandleEvent;
import io.github.alikelleci.eventify.core.event.internal.EventHandlerMethod;
import io.github.alikelleci.eventify.core.handler.annotation.HandleMessage;
import io.github.alikelleci.eventify.core.handler.exception.HandlerRegistrationException;
import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.message.internal.Topics;
import io.github.alikelleci.eventify.core.upcasting.Upcasters;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
  private final Upcasters upcasters = new Upcasters();

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

    upcasters.register(handler);
  }

  public void freeze() {
    frozen = true;
  }

  public boolean isEmpty() {
    return commandHandlers.isEmpty() && eventSourcingHandlers.isEmpty() && eventHandlers.isEmpty() && upcasters.isEmpty();
  }

  /**
   * The aggregate a command handler works on: the one parameter whose type is an aggregate. A command is handled for
   * one aggregate, and Eventify has to know which one before the handler runs, to read that aggregate's events.
   */
  private static Class<?> aggregateOf(Method method) {
    List<Class<?>> aggregates = Arrays.stream(method.getParameterTypes())
        .filter(type -> type.isAnnotationPresent(AggregateRoot.class))
        .distinct()
        .toList();
    if (aggregates.size() != 1) {
      throw new HandlerRegistrationException("A @HandleCommand method takes exactly one aggregate, a parameter whose type is annotated with @AggregateRoot; "
          + method + " takes " + aggregates.size() + ". Eventify needs it to know which aggregate the command belongs to, also when the handler itself does not use it.");
    }
    return aggregates.get(0);
  }

  /** The topics of the commands of one aggregate: the commands another aggregate handles are not its own. */
  public Set<String> commandTopics(String aggregateType) {
    return commandHandlers.entrySet().stream()
        .filter(entry -> entry.getValue().getAggregateType().equals(aggregateType))
        .map(Map.Entry::getKey)
        .collect(Collectors.collectingAndThen(Collectors.toSet(), HandlerRegistry::topicsOf));
  }

  /** The names of the aggregates this instance handles. */
  public Set<String> aggregateTypes() {
    return aggregateClasses().stream().map(AggregateTypes::of).collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * The aggregates the handlers of this instance work on together: the classes annotated with {@link AggregateRoot}
   * that their methods take or return. Empty when no handler names one, e.g. handlers that only take the command.
   */
  public Set<Class<?>> aggregateClasses() {
    return Stream.concat(
            commandHandlers.values().stream().map(CommandHandlerMethod::getMethod),
            eventSourcingHandlers.values().stream().map(ApplyEventMethod::getMethod))
        .flatMap(method -> Stream.concat(Stream.of(method.getReturnType()), Arrays.stream(method.getParameterTypes())))
        .filter(type -> type.isAnnotationPresent(AggregateRoot.class))
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /** The command handler for this command class; {@code null} when there is none. */
  public CommandHandlerMethod commandHandler(Class<?> commandType) {
    return commandHandlers.get(commandType);
  }

  public Map<Class<?>, CommandHandlerMethod> commandHandlers() {
    return Collections.unmodifiableMap(commandHandlers);
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

  /** The upcasters themselves, not a copy: a serde made with them also upcasts with the ones registered after it was made. */
  public Upcasters upcasters() {
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
    requireMessageParameter("@HandleCommand", method);
    Class<?> type = method.getParameters()[0].getType();
    requireTopic("@HandleCommand", type, method);
    String aggregateType = AggregateTypes.of(aggregateOf(method));
    CommandHandlerMethod previous = commandHandlers.put(type, new CommandHandlerMethod(handler, method, aggregateType));
    if (previous != null) {
      requireSameHandler("@HandleCommand", type, previous.getHandler(), previous.getMethod(), handler, method);
    }
  }

  private void addEventSourcingHandler(Object handler, Method method) {
    requireMessageParameter("@ApplyEvent", method);
    Class<?> type = method.getParameters()[0].getType();
    ApplyEventMethod previous = eventSourcingHandlers.put(type, new ApplyEventMethod(handler, method));
    if (previous != null) {
      requireSameHandler("@ApplyEvent", type, previous.getHandler(), previous.getMethod(), handler, method);
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
      throw new HandlerRegistrationException("Two " + annotation + " handlers for " + type.getName() + ": " + previousMethod + " and " + method);
    }
  }

  private void addEventHandler(Object handler, Method method) {
    requireMessageParameter("@HandleEvent", method);
    Class<?> type = method.getParameters()[0].getType();
    requireTopic("@HandleEvent", type, method);
    eventHandlers.put(type, new EventHandlerMethod(handler, method));
  }

  private static void requireMessageParameter(String annotation, Method method) {
    if (method.getParameterCount() == 0) {
      throw new HandlerRegistrationException(annotation + " method must take its message as its first parameter: " + method);
    }
  }

  private static void requireTopic(String annotation, Class<?> messageType, Method method) {
    Topic topic = Topics.of(messageType);
    if (topic == null || StringUtils.isBlank(topic.value())) {
      throw new HandlerRegistrationException(annotation + " method has a message without a @Topic: " + method
          + ". Annotate " + messageType.getName() + ", or an interface it implements, with @Topic.");
    }
  }
}
