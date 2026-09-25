package io.github.alikelleci.eventify.core.upcasting.annotation;

import io.github.alikelleci.eventify.core.handler.annotation.MessageHandler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@MessageHandler
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Upcaster {
  String type();

  int revision();
}
