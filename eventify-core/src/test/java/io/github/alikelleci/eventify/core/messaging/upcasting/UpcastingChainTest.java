package io.github.alikelleci.eventify.core.messaging.upcasting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.Revision;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A stored event is upcast by every upcaster from its revision on, each one taking the payload the one before it left. */
class UpcastingChainTest {

  private static final String TYPE = "io.github.alikelleci.eventify.core.messaging.upcasting.UpcastingChainTest$Renamed";

  /** Revision 1: {@code name}. Revision 2: {@code fullName}. Revision 3: {@code fullName} and {@code country}. */
  @Value
  @Builder
  @Revision(3)
  public static class Renamed {
    @AggregateId
    String id;
    String name;
    String fullName;
    String country;
  }

  /** Each upcaster returns a new node, and leaves the node it was given as it was. */
  public static class ReturningNewNodes {
    @Upcast(type = TYPE, revision = 1)
    public JsonNode nameToFullName(JsonNode payload) {
      ObjectNode upcasted = payload.deepCopy();
      upcasted.set("fullName", upcasted.remove("name"));
      return upcasted;
    }

    @Upcast(type = TYPE, revision = 2)
    public JsonNode addCountry(JsonNode payload) {
      ObjectNode upcasted = payload.deepCopy();
      upcasted.put("country", "NL");
      return upcasted;
    }
  }

  /** Each upcaster changes the node it was given, and returns it. */
  public static class ChangingInPlace {
    @Upcast(type = TYPE, revision = 1)
    public JsonNode nameToFullName(ObjectNode payload) {
      payload.set("fullName", payload.remove("name"));
      return payload;
    }

    @Upcast(type = TYPE, revision = 2)
    public JsonNode addCountry(ObjectNode payload) {
      payload.put("country", "NL");
      return payload;
    }
  }

  @Test
  void upcastersThatReturnNewNodesEachGetTheOneBeforeTheirResult() {
    Event event = read(storedAtRevision1(), new ReturningNewNodes());

    assertThat(event.getRevision()).isEqualTo(3);
    assertThat(event.getPayload()).isEqualTo(Renamed.builder().id("ada").fullName("Ada Lovelace").country("NL").build());
  }

  @Test
  void upcastersThatChangeTheNodeInPlaceGiveTheSameResult() {
    Event event = read(storedAtRevision1(), new ChangingInPlace());

    assertThat(event.getRevision()).isEqualTo(3);
    assertThat(event.getPayload()).isEqualTo(Renamed.builder().id("ada").fullName("Ada Lovelace").country("NL").build());
  }

  @Test
  void anEventStoredAtALaterRevisionOnlyGetsTheUpcastersFromThere() {
    String stored = storedAtRevision1()
        .replace("\"name\":\"Ada Lovelace\"", "\"fullName\":\"Ada Lovelace\"")
        .replace("\"revision\":1", "\"revision\":2");

    Event event = read(stored, new ReturningNewNodes());

    assertThat(event.getRevision()).isEqualTo(3);
    assertThat(event.getPayload()).isEqualTo(Renamed.builder().id("ada").fullName("Ada Lovelace").country("NL").build());
  }

  public static class StoppingAtRevision2 {
    @Upcast(type = TYPE, revision = 1)
    public JsonNode nameToFullName(ObjectNode payload) {
      payload.set("fullName", payload.remove("name"));
      return payload;
    }

    @Upcast(type = TYPE, revision = 2)
    public JsonNode nothing(ObjectNode payload) {
      return null;
    }
  }

  @Test
  void anUpcasterReturningNullStopsTheChainAtItsRevision() {
    Event event = read(storedAtRevision1(), new StoppingAtRevision2());

    assertThat(event.getRevision()).isEqualTo(2);
    assertThat(event.getPayload()).isEqualTo(Renamed.builder().id("ada").fullName("Ada Lovelace").build());
  }

  public static class ReturningText {
    @Upcast(type = TYPE, revision = 1)
    public JsonNode text(ObjectNode payload) {
      return TextNode.valueOf("Ada Lovelace");
    }
  }

  @Test
  void anUpcasterMustReturnAnObject() {
    assertThatThrownBy(() -> read(storedAtRevision1(), new ReturningText()))
        .isInstanceOf(SerializationException.class)
        .rootCause()
        .hasMessageContaining("must return a JSON object");
  }

  public static class TwoForRevision1 {
    @Upcast(type = TYPE, revision = 1)
    public JsonNode one(ObjectNode payload) {
      return payload;
    }

    @Upcast(type = TYPE, revision = 1)
    public JsonNode other(ObjectNode payload) {
      return payload;
    }
  }

  @Test
  void twoUpcastersForTheSameTypeAndRevisionAreRefused() {
    assertThatThrownBy(() -> new JsonDeserializer<>(Event.class).registerUpcaster(new TwoForRevision1()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Two upcasters for " + TYPE + " revision 1");
  }

  /** The event as it was stored at revision 1: with {@code name}, before it was renamed. */
  private static String storedAtRevision1() {
    Event event = Event.builder().payload(Renamed.builder().id("ada").name("Ada Lovelace").build()).build();
    String json = new String(new JsonSerializer<Event>(JacksonUtils.enhancedObjectMapper()).serialize("events", event));
    return json.replace("\"revision\":3", "\"revision\":1");
  }

  private static Event read(String stored, Object upcasters) {
    return new JsonDeserializer<>(Event.class, JacksonUtils.enhancedObjectMapper(), new org.apache.commons.collections4.multimap.ArrayListValuedHashMap<>())
        .registerUpcaster(upcasters)
        .deserialize("events", stored.getBytes());
  }
}
