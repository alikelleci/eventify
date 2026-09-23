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

  /** The annotation on the class, its superclasses or interfaces, also as meta-annotation; {@code null} when absent. */
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
   * The annotated methods, one per signature and the most specific one, so an override isn't run twice.
   * Bridge methods are skipped for the same reason.
   */
  public static <A extends Annotation> List<Method> findAnnotatedMethods(Class<?> clazz, Class<A> annotationClass) {
    if (clazz == null || annotationClass == null) {
      return List.of();
    }

    // Classes first, then interfaces: a class's method is more specific.
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

  /** Also true for a meta-annotation. */
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
