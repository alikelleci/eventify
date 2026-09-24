package io.github.alikelleci.eventify.core.message.internal;

import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.internal.reflection.PerClass;
import io.github.alikelleci.eventify.core.message.annotation.Revision;

import java.util.Optional;
import java.util.function.Function;

/** The {@link Revision} of a class: of an event's payload, or of an aggregate. */
public final class Revisions {

  private static final Function<Class<?>, Integer> REVISION = PerClass.of(annotatedClass ->
      Optional.ofNullable(AnnotationScanner.findAnnotation(annotatedClass, Revision.class))
          .map(Revision::value)
          .orElse(1));

  private Revisions() {
  }

  /** The class's {@link Revision}; 1 without the annotation. */
  public static int of(Class<?> annotatedClass) {
    return REVISION.apply(annotatedClass);
  }
}
