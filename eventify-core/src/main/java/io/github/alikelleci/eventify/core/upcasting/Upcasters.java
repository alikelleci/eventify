package io.github.alikelleci.eventify.core.upcasting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcast;
import io.github.alikelleci.eventify.core.upcasting.exception.UpcastingException;
import io.github.alikelleci.eventify.core.upcasting.internal.UpcasterMethod;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link Upcast} methods of the registered objects, and the chain they form: a stored message is upcast by every
 * upcaster from its revision on, each one taking the payload the one before it left.
 *
 * <p>Upcasters can be added while a serde already reads with them: e.g. Spring registers the upcaster beans after the
 * listener containers are made, but before they start.
 */
public class Upcasters {

  /** By class name, then by the revision an upcaster upcasts from. */
  private final Map<String, Map<Integer, UpcasterMethod>> upcasters = new ConcurrentHashMap<>();

  /**
   * Adds the {@link Upcast} methods of the object. One upcaster per type and revision: with two, the chain would take
   * one of them, depending on the order they were registered in.
   *
   * @throws IllegalStateException when there is already an upcaster for a type and revision of the object
   */
  public Upcasters register(Object handler) {
    AnnotationScanner.findAnnotatedMethods(handler.getClass(), Upcast.class)
        .forEach(method -> add(handler, method));
    return this;
  }

  public boolean isEmpty() {
    return upcasters.isEmpty();
  }

  private void add(Object handler, Method method) {
    if (method.getParameterCount() != 1) {
      return;
    }
    UpcasterMethod upcaster = new UpcasterMethod(handler, method);
    UpcasterMethod existing = upcasters.computeIfAbsent(upcaster.type(), type -> new ConcurrentHashMap<>())
        .putIfAbsent(upcaster.revision(), upcaster);
    if (existing != null) {
      throw new IllegalStateException("Two upcasters for " + upcaster.type() + " revision " + upcaster.revision() + ": " + existing.getMethod() + " and " + method);
    }
  }

  /**
   * Upcasts a stored message, as JSON, to the latest revision of its payload: sets its payload, its revision and, when
   * an upcaster renamed the class, its type. Changes the node it is given, and returns it.
   */
  public JsonNode upcast(JsonNode jsonNode) {
    String storedClassName = classNameOf(jsonNode.get("payload"));
    if (StringUtils.isBlank(storedClassName) || !upcasters.containsKey(storedClassName)) {
      return jsonNode;
    }

    int storedRevision = jsonNode.path("revision").asInt(1);
    int revision = storedRevision;
    String className = storedClassName;
    JsonNode payload = jsonNode.get("payload");

    // Each upcaster takes the payload as the one before it left it: whether it changed the node it was given, or
    // returned a new one. The tree is parsed for this read only, so changing it in place is safe.
    // An upcaster renames the event class by setting another "@class": the chain goes on with the upcasters of that
    // class, from the revision reached. Every step raises the revision, so the chain always ends.
    UpcasterMethod upcaster;
    while ((upcaster = upcasterOf(className, revision)) != null) {
      JsonNode upcasted = upcaster.apply(payload);
      if (upcasted == null) {
        break; // no upcasting from here: the payload stays at this revision
      }
      if (!(upcasted instanceof ObjectNode upcastedObject)) {
        throw new UpcastingException("Upcaster " + upcaster.getMethod() + " must return a JSON object, but returned: " + upcasted.getNodeType());
      }
      String upcastedClassName = classNameOf(upcastedObject);
      if (StringUtils.isBlank(upcastedClassName)) {
        upcastedObject.put("@class", className); // a new node built without it: still the same class
      } else {
        className = upcastedClassName;
      }
      payload = upcastedObject;
      revision++;
    }

    if (revision != storedRevision) {
      ((ObjectNode) jsonNode).set("payload", payload);
      ((ObjectNode) jsonNode).put("revision", revision);
      if (!className.equals(storedClassName) && jsonNode.has("type")) {
        ((ObjectNode) jsonNode).put("type", simpleNameOf(className));
      }
    }
    return jsonNode;
  }

  private UpcasterMethod upcasterOf(String className, int revision) {
    Map<Integer, UpcasterMethod> byRevision = upcasters.get(className);
    return byRevision != null ? byRevision.get(revision) : null;
  }

  private static String classNameOf(JsonNode payload) {
    return Optional.ofNullable(payload)
        .map(node -> node.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);
  }

  /** As {@link Class#getSimpleName()} gives it, without loading the class: "com.example.OrderEvent$OrderPlaced" is "OrderPlaced". */
  private static String simpleNameOf(String className) {
    String name = className.substring(className.lastIndexOf('.') + 1);
    return name.substring(name.lastIndexOf('$') + 1);
  }
}
