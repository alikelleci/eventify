package io.github.alikelleci.eventify.core.aggregate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A snapshot is read also when its aggregate can't be: then it is outdated, and the aggregate is rebuilt. */
@DisplayName("Snapshot serde")
class SnapshotSerdeTest {

  @AggregateRoot("cart")
  public record Cart(@AggregateId String id, int items) {
  }

  private final ObjectMapper objectMapper = EventifyObjectMapper.create();
  private final SnapshotSerde serde = new SnapshotSerde(objectMapper);

  /** A snapshot as the snapshot store holds it: a cart of 3 items at version 40, made with revision 1. */
  private ObjectNode snapshot() {
    ObjectNode payload = objectMapper.valueToTree(new Cart("cart-1", 3));
    payload.put("@class", Cart.class.getName());
    ObjectNode json = objectMapper.createObjectNode();
    json.put("type", "Cart");
    json.set("payload", payload);
    json.put("aggregateId", "cart-1");
    json.put("version", 40);
    json.put("revision", 1);
    return json;
  }

  @Test
  @DisplayName("Should read a snapshot whose aggregate class was moved, without its aggregate")
  void anAggregateClassThatWasMoved() throws Exception {
    ObjectNode json = snapshot();
    ((ObjectNode) json.get("payload")).put("@class", "com.acme.old.Cart");

    AggregateState read = read(json);

    assertThat(read.getPayload()).isNull();
    assertThat(read.getVersion()).isEqualTo(40);
  }

  @Test
  @DisplayName("Should read a snapshot whose aggregate no longer fits its class, without its aggregate")
  void aFieldThatNoLongerFits() throws Exception {
    ObjectNode json = snapshot();
    ((ObjectNode) json.get("payload")).put("items", "three");

    AggregateState read = read(json);

    assertThat(read.getPayload()).isNull();
    assertThat(read.getVersion()).isEqualTo(40);
  }

  private AggregateState read(ObjectNode json) throws Exception {
    return serde.deserializer().deserialize("snapshot-store", objectMapper.writeValueAsBytes(json));
  }
}
