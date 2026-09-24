package io.github.alikelleci.eventify.core.aggregate.internal;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.internal.reflection.PerClass;
import io.github.alikelleci.eventify.core.handler.exception.HandlerRegistrationException;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;

import java.util.function.Function;

/** What an aggregate is called in its stores: the value of its {@link AggregateRoot}. */
public final class AggregateTypes {

  private static final Function<Class<?>, String> NAME = PerClass.of(AggregateTypes::read);

  private AggregateTypes() {
  }

  /** The {@link AggregateRoot} name; throws {@link HandlerRegistrationException} when missing, blank or with NUL. */
  public static String of(Class<?> aggregateClass) {
    return NAME.apply(aggregateClass);
  }

  private static String read(Class<?> aggregateClass) {
    AggregateRoot annotation = aggregateClass.getAnnotation(AggregateRoot.class);
    if (annotation == null) {
      throw new HandlerRegistrationException(aggregateClass.getName() + " is no aggregate: annotate it with @AggregateRoot.");
    }
    String name = annotation.value();
    if (name.isBlank()) {
      throw new HandlerRegistrationException("@AggregateRoot on " + aggregateClass.getName() + " needs a name, e.g. @AggregateRoot(\"order\").");
    }
    if (name.indexOf(StoreKeys.SEPARATOR) >= 0) {
      throw new HandlerRegistrationException("The name of " + aggregateClass.getName() + " cannot contain a NUL character.");
    }
    return name;
  }
}
