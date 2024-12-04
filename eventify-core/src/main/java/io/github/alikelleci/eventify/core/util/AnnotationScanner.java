package io.github.alikelleci.eventify.core.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AnnotationScanner {


  // Find a specific annotation on a class, including its hierarchy and meta-annotations
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

  // Find a specific annotation on a method, including meta-annotations
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

  // Find all methods in the class hierarchy that are annotated with a specific annotation.
  public static <A extends Annotation> List<Method> findAnnotatedMethods(Class<?> clazz, Class<A> annotationClass) {
    Set<Method> methods = new HashSet<>();
    findAnnotatedMethodsRecursive(clazz, annotationClass, methods);
    return new ArrayList<>(methods);
  }

  private static <A extends Annotation> void findAnnotatedMethodsRecursive(
      Class<?> clazz, Class<A> annotationClass, Set<Method> methods) {
    if (clazz == null) {
      return;
    }

    // Check methods in the current class
    for (Method method : clazz.getDeclaredMethods()) {
      if (method.isAnnotationPresent(annotationClass)) {
        methods.add(method);
      } else {
        // Check meta-annotations
        for (Annotation declaredAnnotation : method.getDeclaredAnnotations()) {
          if (declaredAnnotation.annotationType().isAnnotationPresent(annotationClass)) {
            methods.add(method);
            break;
          }
        }
      }
    }

    // Recursively check interfaces
    for (Class<?> iface : clazz.getInterfaces()) {
      findAnnotatedMethodsRecursive(iface, annotationClass, methods);
    }

    // Recursively check superclass
    findAnnotatedMethodsRecursive(clazz.getSuperclass(), annotationClass, methods);
  }
}
