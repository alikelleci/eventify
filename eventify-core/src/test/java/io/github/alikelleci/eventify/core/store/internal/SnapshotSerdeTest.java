package io.github.alikelleci.eventify.core.store.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A snapshot is read also when its aggregate can't be: then it is outdated, and the aggregate is rebuilt. */
@DisplayName("Snapshot serde")
class SnapshotSerdeTest {

  @AggregateRoot
  public record Cart(@AggregateId String id, int items) {
  }

  private final ObjectMapper objectMapper = EventifyObjectMapper.create();
  private final SnapshotSerde serde = new SnapshotSerde(objectMapper);

  private final AggregateState snapshot = AggregateState.builder()
      .payload(new Cart("cart-1", 3))
      .eventId("cart-1@01J00000000000000000000000")
      .version(40)
      .build();

  @Test
  @DisplayName("Should read a snapshot whose aggregate class was moved, without its aggregate")
  void anAggregateClassThatWasMoved() throws Exception {
    ObjectNode json = objectMapper.valueToTree(snapshot);
    ((ObjectNode) json.get("payload")).put("@class", "com.acme.old.Cart");

    AggregateState read = read(json);

    assertThat(read.getPayload()).isNull();
    assertThat(read.getEventId()).isEqualTo(snapshot.getEventId());
    assertThat(read.getVersion()).isEqualTo(40);
    assertThat(SnapshotStore.whyOutdated(read)).contains("can't be read");
  }

  @Test
  @DisplayName("Should read a snapshot whose aggregate no longer fits its class, without its aggregate")
  void aFieldThatNoLongerFits() throws Exception {
    ObjectNode json = objectMapper.valueToTree(snapshot);
    ((ObjectNode) json.get("payload")).put("items", "three");

    AggregateState read = read(json);

    assertThat(read.getPayload()).isNull();
    assertThat(read.getVersion()).isEqualTo(40);
  }

  @Test
  @DisplayName("Should use a snapshot stored before revisions were: it counts as revision 1")
  void aSnapshotWithoutARevisionCountsAsRevision1() throws Exception {
    ObjectNode json = objectMapper.valueToTree(snapshot);
    json.remove("revision");

    AggregateState read = read(json);

    assertThat(read.getPayload()).isEqualTo(new Cart("cart-1", 3));
    assertThat(SnapshotStore.whyOutdated(read)).isNull();
  }

  private AggregateState read(ObjectNode json) throws Exception {
    return serde.deserializer().deserialize("snapshot-store", objectMapper.writeValueAsBytes(json));
  }
}
