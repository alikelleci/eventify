package io.github.alikelleci.eventify.core.message.internal;

import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.message.annotation.Revision;

import java.util.Optional;

/** The {@link Revision} of a class: of an event's payload, or of an aggregate. */
public final class Revisions {

  private Revisions() {
  }

  /** The class's {@link Revision}; 1 without the annotation. */
  public static int of(Class<?> type) {
    return Optional.ofNullable(AnnotationScanner.findAnnotation(type, Revision.class))
        .map(Revision::value)
        .orElse(1);
  }
}
