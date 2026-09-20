package io.github.alikelleci.eventify.core.internal.reflection;

import java.util.function.Function;

/**
 * What a class tells about itself, worked out once and kept: the annotations and fields of a payload class don't
 * change while the application runs, but they are looked up for every message that goes through.
 *
 * <p>Built on {@link ClassValue}, so an entry lives exactly as long as the class it belongs to and is safe to read
 * from every thread without locking.
 */
public final class PerClass {

  private PerClass() {
  }

  /** What {@code answer} says about a class, worked out the first time that class is asked about. */
  public static <T> Function<Class<?>, T> of(Function<Class<?>, T> answer) {
    ClassValue<Holder<T>> cached = new ClassValue<>() {
      @Override
      protected Holder<T> computeValue(Class<?> type) {
        return new Holder<>(answer.apply(type));
      }
    };
    return type -> cached.get(type).value();
  }

  /** Holds the answer, so {@code null} can be an answer too: a ClassValue cannot keep one. */
  private record Holder<T>(T value) {
  }
}
