package io.github.alikelleci.eventify.core.aggregate.internal;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.internal.reflection.PerClass;
import io.github.alikelleci.eventify.core.handler.exception.HandlerRegistrationException;
import io.github.alikelleci.eventify.core.store.StoreKeys;

import java.util.function.Function;

/** What an aggregate is called in its stores: the value of its {@link AggregateRoot}. */
public final class AggregateTypes {

  private static final Function<Class<?>, String> NAME = PerClass.of(AggregateTypes::read);

  private AggregateTypes() {
  }

  /**
   * The name of this aggregate class.
   *
   * @throws HandlerRegistrationException when the class is no aggregate, or its name cannot be part of a store key
   */
  public static String of(Class<?> aggregateType) {
    return NAME.apply(aggregateType);
  }

  private static String read(Class<?> aggregateType) {
    AggregateRoot annotation = aggregateType.getAnnotation(AggregateRoot.class);
    if (annotation == null) {
      throw new HandlerRegistrationException(aggregateType.getName() + " is no aggregate: annotate it with @AggregateRoot.");
    }
    String name = annotation.value();
    if (name.isBlank()) {
      throw new HandlerRegistrationException(aggregateType.getName() + " has no name: @AggregateRoot needs a name of your own choosing, e.g. \"order\", that stays the same when the class is renamed.");
    }
    if (name.indexOf(StoreKeys.SEPARATOR) >= 0) {
      throw new HandlerRegistrationException("The name of " + aggregateType.getName() + " cannot be used: it contains the character that separates the parts of a store key (NUL).");
    }
    return name;
  }
}
