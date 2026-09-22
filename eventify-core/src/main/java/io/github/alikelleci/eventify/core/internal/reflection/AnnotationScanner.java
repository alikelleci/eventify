package io.github.alikelleci.eventify.core.internal.reflection;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AnnotationScanner {

  /**
   * Find a specific annotation on a class or its hierarchy, including interfaces and meta-annotations.
   *
   * @param clazz           the class to search
   * @param annotationClass the annotation type to look for
   * @return the annotation if found, or null if not found
   */
  public static <A extends Annotation> A findAnnotation(Class<?> clazz, Class<A> annotationClass) {
    if (clazz == null || annotationClass == null) {
      return null;
    }

    Set<Class<?>> visited = new HashSet<>();
    while (clazz != null && visited.add(clazz)) {
      // Check directly for the annotation
      A annotation = clazz.getAnnotation(annotationClass);
      if (annotation != null) {
        return annotation;
      }

      // Check meta-annotations
      for (Annotation declaredAnnotation : clazz.getDeclaredAnnotations()) {
        annotation = declaredAnnotation.annotationType().getAnnotation(annotationClass);
        if (annotation != null) {
          return annotation;
        }
      }

      // Check interfaces
      for (Class<?> iface : clazz.getInterfaces()) {
        annotation = findAnnotation(iface, annotationClass);
        if (annotation != null) {
          return annotation;
        }
      }

      // Move up the class hierarchy
      clazz = clazz.getSuperclass();
    }

    return null;
  }

  /**
   * Find a specific annotation on a method, including meta-annotations.
   *
   * @param method          the method to search
   * @param annotationClass the annotation type to look for
   * @return the annotation if found, or null if not found
   */
  public static <A extends Annotation> A findAnnotation(Method method, Class<A> annotationClass) {
    if (method == null || annotationClass == null) {
      return null;
    }

    // Check directly for the annotation
    A annotation = method.getAnnotation(annotationClass);
    if (annotation != null) {
      return annotation;
    }

    // Check meta-annotations
    for (Annotation declaredAnnotation : method.getDeclaredAnnotations()) {
      annotation = declaredAnnotation.annotationType().getAnnotation(annotationClass);
      if (annotation != null) {
        return annotation;
      }
    }

    return null;
  }

  /**
   * Find all methods in a class hierarchy annotated with a specific annotation: one per signature. An annotated method
   * and an annotated method it overrides are the same handler, and a reflective call of either runs the override, so
   * returning both would run it twice. The most specific one is returned: of the class itself, then of its
   * superclasses, then of the interfaces. Bridge methods, which the compiler adds for a generic override and which carry
   * its annotations, are skipped for the same reason.
   *
   * @param clazz           the class to search
   * @param annotationClass the annotation type to look for
   * @return a list of methods annotated with the specified annotation
   */
  public static <A extends Annotation> List<Method> findAnnotatedMethods(Class<?> clazz, Class<A> annotationClass) {
    if (clazz == null || annotationClass == null) {
      return List.of();
    }

    // The class and its superclasses first, then all their interfaces: a class's method is more specific than any
    // interface method it implements.
    List<Class<?>> hierarchy = new ArrayList<>();
    for (Class<?> type = clazz; type != null; type = type.getSuperclass()) {
      hierarchy.add(type);
    }
    Set<Class<?>> visited = new HashSet<>(hierarchy);
    for (int i = 0; i < hierarchy.size(); i++) {
      for (Class<?> iface : hierarchy.get(i).getInterfaces()) {
        if (visited.add(iface)) {
          hierarchy.add(iface);
        }
      }
    }

    Map<List<Object>, Method> methods = new LinkedHashMap<>();
    for (Class<?> type : hierarchy) {
      for (Method method : type.getDeclaredMethods()) {
        if (!method.isBridge() && isAnnotatedWith(method, annotationClass)) {
          methods.putIfAbsent(List.of(method.getName(), List.of(method.getParameterTypes())), method);
        }
      }
    }

    return new ArrayList<>(methods.values());
  }

  /**
   * Helper to check if a method is annotated with a specific annotation, including meta-annotations.
   */
  private static <A extends Annotation> boolean isAnnotatedWith(Method method, Class<A> annotationClass) {
    if (method.isAnnotationPresent(annotationClass)) {
      return true;
    }

    // Check meta-annotations
    for (Annotation declaredAnnotation : method.getDeclaredAnnotations()) {
      if (declaredAnnotation.annotationType().isAnnotationPresent(annotationClass)) {
        return true;
      }
    }

    return false;
  }
}
