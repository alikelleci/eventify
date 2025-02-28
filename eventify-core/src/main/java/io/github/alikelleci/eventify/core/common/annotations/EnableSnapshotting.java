package io.github.alikelleci.eventify.core.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface EnableSnapshotting {
  int threshold() default 500;

  boolean deleteEvents() default false;
}
