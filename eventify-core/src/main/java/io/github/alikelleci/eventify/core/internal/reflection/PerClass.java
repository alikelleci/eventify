package io.github.alikelleci.eventify.core.internal.reflection;

import java.util.function.Function;

/** Caches a per-class answer in a {@link ClassValue}: thread-safe, and freed with the class. */
public final class PerClass {

  private PerClass() {
  }

  /** {@code answer}, computed once per class. */
  public static <T> Function<Class<?>, T> of(Function<Class<?>, T> answer) {
    ClassValue<Holder<T>> cached = new ClassValue<>() {
      @Override
      protected Holder<T> computeValue(Class<?> type) {
        return new Holder<>(answer.apply(type));
      }
    };
    return type -> cached.get(type).value();
  }

  /** So {@code null} can be cached too. */
  private record Holder<T>(T value) {
  }
}
