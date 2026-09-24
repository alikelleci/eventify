package io.github.alikelleci.eventify.core.message.internal;

import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.internal.reflection.PerClass;
import io.github.alikelleci.eventify.core.message.annotation.Topic;

import java.util.function.Function;

/** The {@link Topic} a payload class names, or the one of an interface it implements; {@code null} when it names none. */
public final class Topics {

  private static final Function<Class<?>, Topic> TOPIC = PerClass.of(payloadClass -> AnnotationScanner.findAnnotation(payloadClass, Topic.class));

  private Topics() {
  }

  public static Topic of(Class<?> payloadClass) {
    return TOPIC.apply(payloadClass);
  }
}
