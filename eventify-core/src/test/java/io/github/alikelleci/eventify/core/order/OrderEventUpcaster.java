package io.github.alikelleci.eventify.core.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.messaging.upcasting.annotations.Upcast;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderEventUpcaster {

  @Upcast(type = "io.github.alikelleci.eventify.core.domain.OrderEvent$OrderPlaced", revision = 1)
  public JsonNode upcastRev1(ObjectNode objectNode) {
    objectNode.put("shippingAddress", "unknown");
    return objectNode;
  }

  @Upcast(type = "io.github.alikelleci.eventify.core.domain.OrderEvent$OrderPlaced", revision = 2)
  public JsonNode upcastRev2(ObjectNode objectNode) {
    objectNode.putNull("couponCode");
    return objectNode;
  }
}
