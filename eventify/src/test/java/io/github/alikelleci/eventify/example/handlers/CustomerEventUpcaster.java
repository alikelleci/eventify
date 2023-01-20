package io.github.alikelleci.eventify.example.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.messaging.upcasting.annotations.Upcast;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomerEventUpcaster {

  @Upcast(type = "io.github.alikelleci.eventify.example.domain.CustomerEvent$CustomerCreated", revision = 1)
  public JsonNode upcastRev1(ObjectNode objectNode) {
    objectNode.put("firstName", "John v1 -> v2");
    log.info("Upcasted from revision 1 to 2");

    return objectNode;
  }

  @Upcast(type = "io.github.alikelleci.eventify.example.domain.CustomerEvent$CustomerCreated", revision = 2)
  public JsonNode upcastRev2(ObjectNode objectNode) {
    objectNode.put("firstName", "John v2 -> v3");
    log.info("Upcasted from revision 2 to 3");

    return objectNode;
  }

  @Upcast(type = "io.github.alikelleci.eventify.example.domain.CustomerEvent$CustomerCreated", revision = 3)
  public JsonNode upcastRev3(ObjectNode objectNode) {
    objectNode.put("firstName", "John v3 -> v4");
    log.info("Upcasted from revision 3 to 4");

    return objectNode;
  }

  @Upcast(type = "io.github.alikelleci.eventify.example.domain.CustomerEvent$CustomerCreated", revision = 5)
  public JsonNode upcastRev6(ObjectNode objectNode) {
    objectNode.put("firstName", "John v6 -> v7");
    log.info("Upcasted from revision 6 to 7");

    return objectNode;
  }

  @Upcast(type = "io.github.alikelleci.eventify.example.domain.CustomerEvent$CustomerCreated", revision = 6)
  public JsonNode upcastRev7(ObjectNode objectNode) {
    objectNode.put("firstName", "John v7 -> v8");
    log.info("Upcasted from revision 7 to 8");

    return objectNode;
  }
}
