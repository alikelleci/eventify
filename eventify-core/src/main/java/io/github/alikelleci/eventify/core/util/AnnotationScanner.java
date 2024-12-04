package io.github.alikelleci.eventify.core.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class AnnotationScanner {

  /**
   * Find an annotation on a class or its hierarchy (including superclasses, interfaces, and meta-annotations).
   *
   * @param clazz           the class to search
   * @param annotationClass the annotation type to look for
   * @return the annotation if found, or null if not found
   */
  public static <A extends Annotation> A findAnnotation(Class<?> clazz, Class<A> annotationClass) {
    if (clazz == null) {
      return null;
    }

    // Check if the annotation is directly present on the class
    A annotation = clazz.getAnnotation(annotationClass);
    if (annotation != null) {
      return annotation;
    }

    // Check for meta-annotations
    for (Annotation declaredAnnotation : clazz.getDeclaredAnnotations()) {
      annotation = declaredAnnotation.annotationType().getAnnotation(annotationClass);
      if (annotation != null) {
        return annotation;
      }
    }

    // Recursively check interfaces
    for (Class<?> iface : clazz.getInterfaces()) {
      annotation = findAnnotation(iface, annotationClass);
      if (annotation != null) {
        return annotation;
      }
    }

    // Recursively check superclass
    return findAnnotation(clazz.getSuperclass(), annotationClass);
  }

  /**
   * Find an annotation on a method or its meta-annotations.
   *
   * @param method          the method to search
   * @param annotationClass the annotation type to look for
   * @return the annotation if found, or null if not found
   */
  public static <A extends Annotation> A findAnnotation(Method method, Class<A> annotationClass) {
    if (method == null) {
      return null;
    }

    // Check if the annotation is directly present on the method
    A annotation = method.getAnnotation(annotationClass);
    if (annotation != null) {
      return annotation;
    }

    // Check for meta-annotations
    for (Annotation declaredAnnotation : method.getDeclaredAnnotations()) {
      annotation = declaredAnnotation.annotationType().getAnnotation(annotationClass);
      if (annotation != null) {
        return annotation;
      }
    }

    return null;
  }
}
