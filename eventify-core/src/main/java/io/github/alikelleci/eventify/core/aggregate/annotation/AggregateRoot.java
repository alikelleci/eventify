package io.github.alikelleci.eventify.core.aggregate.annotation;

import com.fasterxml.jackson.annotation.JacksonAnnotationsInside;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Inherited
@JacksonAnnotationsInside
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public @interface AggregateRoot {

  /**
   * The aggregate's name in the stores, e.g. "order": your own name, not the class name, so a rename moves no data.
   * Unique within an application.
   */
  String value();
}
