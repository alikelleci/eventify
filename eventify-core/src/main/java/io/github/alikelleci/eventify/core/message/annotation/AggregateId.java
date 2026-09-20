package io.github.alikelleci.eventify.core.message.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The field that says which aggregate a command or event is about. Every command and event class has exactly one, and
 * its value is the aggregate's identifier: a {@code String}, a {@code UUID} or a number.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AggregateId {
}
