package io.github.alikelleci.eventify.core.handler.internal;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.aggregate.internal.AggregateTypes;
import io.github.alikelleci.eventify.core.aggregate.internal.ApplyEventMethod;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.command.internal.CommandHandlerMethod;
import io.github.alikelleci.eventify.core.event.annotation.HandleEvent;
import io.github.alikelleci.eventify.core.event.internal.EventHandlerMethod;
import io.github.alikelleci.eventify.core.handler.HandlerParameterResolver;
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
import java.lang.reflect.Parameter;
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

/** The handlers and upcasters of one Eventify instance; never changed after construction. */
public class HandlerRegistry {

  private final Map<Class<?>, CommandHandlerMethod> commandHandlers = new HashMap<>();
  private final Map<Class<?>, ApplyEventMethod> eventSourcingHandlers = new HashMap<>();
  private final MultiValuedMap<Class<?>, EventHandlerMethod> eventHandlers = new ArrayListValuedHashMap<>();
  private final Upcasters upcasters;

  public HandlerRegistry(List<Object> handlers) {
    handlers.forEach(this::register);
    upcasters = Upcasters.of(handlers);
    requireDistinctAggregateTypes();
  }

  /** Whether the class has a method with an Eventify handler annotation, e.g. {@code @HandleCommand} or {@code @Upcast}. */
  public static boolean isHandler(Class<?> handlerClass) {
    return !AnnotationScanner.findAnnotatedMethods(handlerClass, HandleMessage.class).isEmpty();
  }

  /** The names of the aggregates this instance handles. */
  public Set<String> aggregateTypes() {
    return aggregateClasses().stream().map(AggregateTypes::of).collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /** The {@link AggregateRoot} classes the handlers take or return. */
  public Set<Class<?>> aggregateClasses() {
    return Stream.concat(
            commandHandlers.values().stream().map(CommandHandlerMethod::getMethod),
            eventSourcingHandlers.values().stream().map(ApplyEventMethod::getMethod))
        .flatMap(method -> Stream.concat(Stream.of(method.getReturnType()), Arrays.stream(method.getParameterTypes())))
        .filter(parameterClass -> parameterClass.isAnnotationPresent(AggregateRoot.class))
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /** The command handler for this command class; {@code null} when there is none. */
  public CommandHandlerMethod commandHandler(Class<?> commandClass) {
    return commandHandlers.get(commandClass);
  }

  public Map<Class<?>, CommandHandlerMethod> commandHandlers() {
    return Collections.unmodifiableMap(commandHandlers);
  }

  /** The command topics of one aggregate. */
  public Set<String> commandTopics(String aggregateType) {
    return commandHandlers.entrySet().stream()
        .filter(entry -> entry.getValue().getAggregateType().equals(aggregateType))
        .map(Map.Entry::getKey)
        .collect(Collectors.collectingAndThen(Collectors.toSet(), HandlerRegistry::topicsOf));
  }

  /** The topics of the commands that have a command handler. */
  public Set<String> commandTopics() {
    return topicsOf(commandHandlers.keySet());
  }

  public Map<Class<?>, ApplyEventMethod> eventSourcingHandlers() {
    return Collections.unmodifiableMap(eventSourcingHandlers);
  }

  public Collection<EventHandlerMethod> eventHandlers() {
    return Collections.unmodifiableCollection(eventHandlers.values());
  }

  /** The event handlers for this event class, in the order they were registered; empty when there are none. */
  public Collection<EventHandlerMethod> eventHandlers(Class<?> eventClass) {
    return Collections.unmodifiableCollection(eventHandlers.get(eventClass));
  }

  public boolean hasEventHandlers() {
    return !eventHandlers.isEmpty();
  }

  /** The topics of the events that have an event handler. */
  public Set<String> eventTopics() {
    return topicsOf(eventHandlers.keySet());
  }

  public Upcasters upcasters() {
    return upcasters;
  }

  private void register(Object handler) {
    AnnotationScanner.findAnnotatedMethods(handler.getClass(), HandleCommand.class)
        .forEach(method -> addCommandHandler(handler, method));

    AnnotationScanner.findAnnotatedMethods(handler.getClass(), ApplyEvent.class)
        .forEach(method -> addEventSourcingHandler(handler, method));

    AnnotationScanner.findAnnotatedMethods(handler.getClass(), HandleEvent.class)
        .forEach(method -> addEventHandler(handler, method));
  }

  /** The one {@code @AggregateRoot} parameter: Eventify must know which aggregate to load before the handler runs. */
  private static Class<?> aggregateOf(Method method) {
    List<Class<?>> aggregates = aggregateParameterClasses(method);
    if (aggregates.size() != 1) {
      throw new HandlerRegistrationException("@HandleCommand method " + method + " must take exactly one @AggregateRoot parameter, not " + aggregates.size() + ".");
    }
    return aggregates.get(0);
  }

  private static Set<String> topicsOf(Set<Class<?>> messageClasses) {
    return messageClasses.stream()
        .map(Topics::of)
        .filter(Objects::nonNull)
        .map(Topic::value)
        .collect(Collectors.toSet());
  }

  private void addCommandHandler(Object handler, Method method) {
    requireMessageParameter("@HandleCommand", method);
    requireSupportedParameters("@HandleCommand", method, true);
    Class<?> messageClass = method.getParameters()[0].getType();
    requireTopicOnHandledMessage("@HandleCommand", messageClass, method);
    String aggregateType = AggregateTypes.of(aggregateOf(method));
    CommandHandlerMethod previous = commandHandlers.put(messageClass, new CommandHandlerMethod(handler, method, aggregateType));
    if (previous != null) {
      requireSingleHandler("@HandleCommand", messageClass, previous.getHandler(), previous.getMethod(), handler, method);
    }
  }

  private void addEventSourcingHandler(Object handler, Method method) {
    requireMessageParameter("@ApplyEvent", method);
    requireSupportedParameters("@ApplyEvent", method, true);
    requireMatchingAggregateReturnType(method);
    Class<?> messageClass = method.getParameters()[0].getType();
    ApplyEventMethod previous = eventSourcingHandlers.put(messageClass, new ApplyEventMethod(handler, method));
    if (previous != null) {
      requireSingleHandler("@ApplyEvent", messageClass, previous.getHandler(), previous.getMethod(), handler, method);
    }
  }

  private void addEventHandler(Object handler, Method method) {
    requireMessageParameter("@HandleEvent", method);
    requireSupportedParameters("@HandleEvent", method, false);
    Class<?> messageClass = method.getParameters()[0].getType();
    requireTopicOnHandledMessage("@HandleEvent", messageClass, method);
    // The same object registered twice is still one handler.
    boolean registered = eventHandlers.get(messageClass).stream()
        .anyMatch(previous -> previous.getHandler() == handler && previous.getMethod().equals(method));
    if (!registered) {
      eventHandlers.put(messageClass, new EventHandlerMethod(handler, method));
    }
  }

  /** Aggregate names must be unique: the name is what keeps their keys apart. Also checks each name is valid. */
  private void requireDistinctAggregateTypes() {
    Map<String, Class<?>> byType = new HashMap<>();
    for (Class<?> aggregate : aggregateClasses()) {
      String type = AggregateTypes.of(aggregate);
      Class<?> previous = byType.put(type, aggregate);
      if (previous != null) {
        throw new HandlerRegistrationException("Aggregates " + previous.getName() + " and " + aggregate.getName()
            + " are both named '" + type + "'. Give each aggregate its own @AggregateRoot name.");
      }
    }
  }

  private static void requireMessageParameter(String annotation, Method method) {
    if (method.getParameterCount() == 0) {
      throw new HandlerRegistrationException(annotation + " method must take its message as its first parameter: " + method);
    }
  }

  /** Every parameter after the message must be one Eventify has a value for. */
  private static void requireSupportedParameters(String annotation, Method method, boolean takesAggregate) {
    Parameter[] parameters = method.getParameters();
    for (int i = 1; i < parameters.length; i++) {
      boolean aggregate = takesAggregate && parameters[i].getType().isAnnotationPresent(AggregateRoot.class);
      if (!aggregate && !HandlerParameterResolver.supports(parameters[i])) {
        throw new HandlerRegistrationException(annotation + " method " + method + " has an unsupported parameter: " + parameters[i]);
      }
    }
  }

  /** Throws on a second handler; the same object twice, or an override of the annotated method, is allowed. */
  private static void requireSingleHandler(String annotation, Class<?> messageClass, Object previousHandler, Method previousMethod, Object handler, Method method) {
    boolean sameHandler = previousHandler == handler
        && previousMethod.getName().equals(method.getName())
        && Arrays.equals(previousMethod.getParameterTypes(), method.getParameterTypes());
    if (!sameHandler) {
      throw new HandlerRegistrationException("Two " + annotation + " handlers for " + messageClass.getName() + ": " + previousMethod + " and " + method);
    }
  }

  /** An apply method must return the type of its {@code @AggregateRoot} parameter: caught here, not at replay. */
  private static void requireMatchingAggregateReturnType(Method method) {
    Class<?> mismatchingAggregate = aggregateParameterClasses(method).stream()
        .filter(aggregate -> method.getReturnType() != aggregate)
        .findFirst()
        .orElse(null);
    if (mismatchingAggregate != null) {
      throw new HandlerRegistrationException("@ApplyEvent method " + method + " must return " + mismatchingAggregate.getName() + ", its @AggregateRoot parameter.");
    }
  }

  private static void requireTopicOnHandledMessage(String annotation, Class<?> messageClass, Method method) {
    Topic topic = Topics.of(messageClass);
    if (topic == null || StringUtils.isBlank(topic.value())) {
      throw new HandlerRegistrationException(annotation + " method " + method + " handles " + messageClass.getName() + ", which has no @Topic.");
    }
  }

  private static List<Class<?>> aggregateParameterClasses(Method method) {
    return Arrays.stream(method.getParameterTypes())
        .filter(parameterClass -> parameterClass.isAnnotationPresent(AggregateRoot.class))
        .distinct()
        .toList();
  }
}
