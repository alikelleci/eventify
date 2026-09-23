package io.github.alikelleci.eventify.core.message.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** The field holding the aggregate id: exactly one per command and event class; its {@code toString()} is the id. */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AggregateId {
}
