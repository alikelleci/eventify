package io.github.alikelleci.eventify.core.internal;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.aggregate.internal.AggregateTypes;
import io.github.alikelleci.eventify.core.aggregate.internal.EventSourcingHandlerMethod;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.command.internal.CommandHandlerMethod;
import io.github.alikelleci.eventify.core.event.annotation.EventHandler;
import io.github.alikelleci.eventify.core.event.internal.EventHandlerMethod;
import io.github.alikelleci.eventify.core.handler.HandlerParameterResolver;
import io.github.alikelleci.eventify.core.handler.annotation.MessageHandler;
import io.github.alikelleci.eventify.core.handler.annotation.MessageId;
import io.github.alikelleci.eventify.core.handler.annotation.MetadataValue;
import io.github.alikelleci.eventify.core.handler.annotation.Sequence;
import io.github.alikelleci.eventify.core.handler.annotation.Timestamp;
import io.github.alikelleci.eventify.core.handler.exception.HandlerRegistrationException;
import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.message.internal.Topics;
import io.github.alikelleci.eventify.core.upcasting.Upcasters;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** The handlers and upcasters of one Eventify instance; never changed after construction. */
public class HandlerRegistry {

  private final Map<Class<?>, CommandHandlerMethod> commandHandlers = new HashMap<>();
  private final Map<Class<?>, EventSourcingHandlerMethod> eventSourcingHandlers = new HashMap<>();
  private final MultiValuedMap<Class<?>, EventHandlerMethod> eventHandlers = new ArrayListValuedHashMap<>();
  private final Upcasters upcasters;

  public HandlerRegistry(List<Object> handlers) {
    handlers.forEach(this::register);
    upcasters = Upcasters.of(handlers);
    requireDistinctAggregateTypes();
  }

  /** Whether the class has a method with an Eventify handler annotation, e.g. {@code @CommandHandler} or {@code @Upcaster}. */
  public static boolean isHandler(Class<?> handlerClass) {
    return !AnnotationScanner.findAnnotatedMethods(handlerClass, MessageHandler.class).isEmpty();
  }

  /** The names of the aggregates this instance handles. */
  public Set<String> aggregateTypes() {
    return aggregateClasses().stream().map(AggregateTypes::of).collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /** The {@link AggregateRoot} classes the handlers take or return. */
  public Set<Class<?>> aggregateClasses() {
    return Stream.concat(
            commandHandlers.values().stream().map(CommandHandlerMethod::getMethod),
            eventSourcingHandlers.values().stream().map(EventSourcingHandlerMethod::getMethod))
        .flatMap(method -> Stream.concat(Stream.of(method.getReturnType()), Arrays.stream(method.getParameterTypes())))
        .filter(parameterClass -> parameterClass.isAnnotationPresent(AggregateRoot.class))
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /** The {@link AggregateRoot} class with this name; {@code null} when there is none. */
  public Class<?> aggregateClass(String aggregateType) {
    return aggregateClasses().stream()
        .filter(aggregateClass -> AggregateTypes.of(aggregateClass).equals(aggregateType))
        .findFirst()
        .orElse(null);
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

  public Map<Class<?>, EventSourcingHandlerMethod> eventSourcingHandlers() {
    return Collections.unmodifiableMap(eventSourcingHandlers);
  }

  public Collection<EventHandlerMethod> eventHandlers() {
    return Collections.unmodifiableCollection(eventHandlers.values());
  }

  /** The event handlers for this event class, in the order they were registered; empty when there are none. */
  public Collection<EventHandlerMethod> eventHandlers(Class<?> eventClass) {
    return Collections.unmodifiableCollection(eventHandlers.get(eventClass));
  }

  /** The event classes that have an event handler. */
  public Set<Class<?>> eventClasses() {
    return Collections.unmodifiableSet(eventHandlers.keySet());
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
    AnnotationScanner.findAnnotatedMethods(handler.getClass(), CommandHandler.class)
        .forEach(method -> addCommandHandler(handler, method));

    AnnotationScanner.findAnnotatedMethods(handler.getClass(), EventSourcingHandler.class)
        .forEach(method -> addEventSourcingHandler(handler, method));

    AnnotationScanner.findAnnotatedMethods(handler.getClass(), EventHandler.class)
        .forEach(method -> addEventHandler(handler, method));
  }

  /** The one {@code @AggregateRoot} parameter: Eventify must know which aggregate to load before the handler runs. */
  private static Class<?> aggregateOf(Method method) {
    List<Class<?>> aggregates = aggregateParameterClasses(method);
    if (aggregates.size() != 1) {
      throw new HandlerRegistrationException("@CommandHandler method " + method + " must take exactly one @AggregateRoot parameter, not " + aggregates.size() + ".");
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
    requireNotPrivate("@CommandHandler", method);
    requireMessageParameter("@CommandHandler", method);
    if (Arrays.stream(method.getParameters()).anyMatch(parameter -> parameter.isAnnotationPresent(Sequence.class))) {
      throw new HandlerRegistrationException("@CommandHandler method " + method + " has a @Sequence parameter: a command has no sequence.");
    }
    requireSupportedParameters("@CommandHandler", method, true, false);
    Class<?> messageClass = method.getParameters()[0].getType();
    requireTopicOnHandledMessage("@CommandHandler", messageClass, method);
    String aggregateType = AggregateTypes.of(aggregateOf(method));
    CommandHandlerMethod previous = commandHandlers.put(messageClass, new CommandHandlerMethod(handler, method, aggregateType));
    if (previous != null) {
      requireSingleHandler("@CommandHandler", messageClass, previous.getHandler(), previous.getMethod(), handler, method);
    }
  }

  private void addEventSourcingHandler(Object handler, Method method) {
    requireNotPrivate("@EventSourcingHandler", method);
    requireMessageParameter("@EventSourcingHandler", method);
    requireSupportedParameters("@EventSourcingHandler", method, true, true);
    requireMatchingAggregateReturnType(method);
    Class<?> messageClass = method.getParameters()[0].getType();
    EventSourcingHandlerMethod previous = eventSourcingHandlers.put(messageClass, new EventSourcingHandlerMethod(handler, method));
    if (previous != null) {
      requireSingleHandler("@EventSourcingHandler", messageClass, previous.getHandler(), previous.getMethod(), handler, method);
    }
  }

  private void addEventHandler(Object handler, Method method) {
    requireNotPrivate("@EventHandler", method);
    requireMessageParameter("@EventHandler", method);
    requireSupportedParameters("@EventHandler", method, false, true);
    Class<?> messageClass = method.getParameters()[0].getType();
    requireTopicOnHandledMessage("@EventHandler", messageClass, method);
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

  /** A proxy, e.g. Spring's for @Transactional, can't override a private method: it would run without the bean's fields. */
  private static void requireNotPrivate(String annotation, Method method) {
    if (Modifier.isPrivate(method.getModifiers())) {
      throw new HandlerRegistrationException(annotation + " method " + method + " is private: make it package-private or public.");
    }
  }

  private static void requireMessageParameter(String annotation, Method method) {
    if (method.getParameterCount() == 0) {
      throw new HandlerRegistrationException(annotation + " method must take its message as its first parameter: " + method);
    }
    // Handlers are found by the message's exact class: one on an interface would never be called.
    Class<?> messageClass = method.getParameterTypes()[0];
    if (messageClass.isInterface() || Modifier.isAbstract(messageClass.getModifiers())) {
      throw new HandlerRegistrationException(annotation + " method " + method + " takes " + messageClass.getName()
          + ", an interface or abstract class: take the message class itself.");
    }
  }

  /** Every parameter after the message must be one Eventify has a value for. */
  private static void requireSupportedParameters(String annotation, Method method, boolean takesAggregate, boolean takesSequence) {
    Parameter[] parameters = method.getParameters();
    for (int i = 1; i < parameters.length; i++) {
      boolean aggregate = takesAggregate && parameters[i].getType().isAnnotationPresent(AggregateRoot.class);
      boolean supported = parameters[i].isAnnotationPresent(Sequence.class)
          ? takesSequence && isSequence(parameters[i])
          : aggregate || HandlerParameterResolver.supports(parameters[i]);
      if (!supported) {
        throw new HandlerRegistrationException(annotation + " method " + method + " has an unsupported parameter: " + parameters[i]);
      }
    }
  }

  /** A {@code @Sequence long} or {@code Long} without another message annotation; events only, so not in the generic resolver. */
  private static boolean isSequence(Parameter parameter) {
    Class<?> type = parameter.getType();
    boolean otherValue = parameter.isAnnotationPresent(Timestamp.class) || parameter.isAnnotationPresent(MessageId.class)
        || parameter.isAnnotationPresent(MetadataValue.class);
    return !otherValue && (type == long.class || type.isAssignableFrom(Long.class));
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

  /** An event-sourcing handler must return an aggregate, the type of its {@code @AggregateRoot} parameter: caught here, not at replay. */
  private static void requireMatchingAggregateReturnType(Method method) {
    // Also void: it would return null, which removes the aggregate on every replay.
    if (!method.getReturnType().isAnnotationPresent(AggregateRoot.class)) {
      throw new HandlerRegistrationException("@EventSourcingHandler method " + method + " must return an @AggregateRoot class, not " + method.getReturnType().getName() + ".");
    }
    Class<?> mismatchingAggregate = aggregateParameterClasses(method).stream()
        .filter(aggregate -> method.getReturnType() != aggregate)
        .findFirst()
        .orElse(null);
    if (mismatchingAggregate != null) {
      throw new HandlerRegistrationException("@EventSourcingHandler method " + method + " must return " + mismatchingAggregate.getName() + ", its @AggregateRoot parameter.");
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
