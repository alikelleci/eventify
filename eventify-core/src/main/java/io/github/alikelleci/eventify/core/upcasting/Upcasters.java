package io.github.alikelleci.eventify.core.upcasting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.handler.exception.HandlerRegistrationException;
import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcast;
import io.github.alikelleci.eventify.core.upcasting.exception.UpcastingException;
import io.github.alikelleci.eventify.core.upcasting.internal.UpcasterMethod;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The {@link Upcast} chain: a stored message goes through every upcaster from its revision on. Immutable. */
public final class Upcasters {

  /** No upcasters: messages are read as stored. */
  public static final Upcasters NONE = new Upcasters(List.of(), Map.of());

  /** Kept so {@link #with} can add to them. */
  private final List<Object> handlers;
  /** By class name, then by the revision an upcaster upcasts from. */
  private final Map<String, Map<Integer, UpcasterMethod>> upcasters;

  private Upcasters(List<Object> handlers, Map<String, Map<Integer, UpcasterMethod>> upcasters) {
    this.handlers = handlers;
    this.upcasters = upcasters;
  }

  /** The {@link Upcast} methods of these objects; two for one type and revision throw {@link HandlerRegistrationException}. */
  public static Upcasters of(Collection<?> handlers) {
    Map<String, Map<Integer, UpcasterMethod>> upcasters = new HashMap<>();
    handlers.forEach(handler -> AnnotationScanner.findAnnotatedMethods(handler.getClass(), Upcast.class)
        .forEach(method -> add(upcasters, handler, method)));
    Map<String, Map<Integer, UpcasterMethod>> copy = new HashMap<>();
    upcasters.forEach((type, byRevision) -> copy.put(type, Map.copyOf(byRevision)));
    return new Upcasters(List.copyOf(handlers), Map.copyOf(copy));
  }

  /** New upcasters with these objects added. */
  public Upcasters with(Object... handlers) {
    List<Object> all = new ArrayList<>(this.handlers);
    all.addAll(List.of(handlers));
    return of(all);
  }

  public boolean isEmpty() {
    return upcasters.isEmpty();
  }

  private static void add(Map<String, Map<Integer, UpcasterMethod>> upcasters, Object handler, Method method) {
    requireJsonSignature(method);
    UpcasterMethod upcaster = new UpcasterMethod(handler, method);
    UpcasterMethod existing = upcasters.computeIfAbsent(upcaster.getType(), type -> new HashMap<>())
        .putIfAbsent(upcaster.getRevision(), upcaster);
    if (existing != null) {
      throw new HandlerRegistrationException("Two upcasters for " + upcaster.getType() + " revision " + upcaster.getRevision() + ": " + existing.getMethod() + " and " + method);
    }
  }

  private static void requireJsonSignature(Method method) {
    boolean takesJson = method.getParameterCount() == 1 && method.getParameterTypes()[0].isAssignableFrom(ObjectNode.class);
    boolean returnsJson = JsonNode.class.isAssignableFrom(method.getReturnType());
    if (!takesJson || !returnsJson) {
      throw new HandlerRegistrationException("An @Upcast method must take the payload as a JsonNode or ObjectNode and return a JsonNode: " + method);
    }
  }

  /** Upcasts the message JSON in place to the latest revision: payload, revision and, when renamed, type. */
  public JsonNode upcast(JsonNode jsonNode) {
    String storedClassName = classNameOf(jsonNode.get("payload"));
    if (StringUtils.isBlank(storedClassName) || !upcasters.containsKey(storedClassName)) {
      return jsonNode;
    }

    int storedRevision = jsonNode.path("revision").asInt(1);
    int revision = storedRevision;
    String className = storedClassName;
    JsonNode payload = jsonNode.get("payload");

    // Each step takes the previous result and raises the revision, so the chain ends; a new "@class" renames the type.
    UpcasterMethod upcaster;
    while ((upcaster = upcasterOf(className, revision)) != null) {
      JsonNode upcasted = upcaster.handle(payload);
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
