package io.github.alikelleci.eventify.core.messaging.upcasting.annotations;

import io.github.alikelleci.eventify.core.common.annotations.HandleMessage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@HandleMessage
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Upcast {
  String type();

  int revision();
}
