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
   * What this aggregate is called in its stores, e.g. "order". It is part of the key of every event and snapshot, so
   * it is a name of your own choosing and NOT the class name: renaming the class must not move the data. Two
   * aggregates of one application cannot share a name.
   */
  String value();
}
