package io.github.alikelleci.eventify.core.upcasting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.handler.exception.HandlerRegistrationException;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Revision;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcast;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A stored event is upcast by every upcaster from its revision on, each one taking the payload the one before it left. */
@DisplayName("Upcasting chain")
class UpcastingChainTest {

  private static final String TYPE = "io.github.alikelleci.eventify.core.upcasting.UpcastingChainTest$Renamed";

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
  @DisplayName("Should give each upcaster the result of the one before it, when upcasters return new nodes")
  void upcastersThatReturnNewNodesEachGetTheOneBeforeTheirResult() {
    Event event = read(storedAtRevision1(), new ReturningNewNodes());

    assertThat(event.getRevision()).isEqualTo(3);
    assertThat(event.getPayload()).isEqualTo(Renamed.builder().id("ada").fullName("Ada Lovelace").country("NL").build());
  }

  @Test
  @DisplayName("Should give the same result when upcasters change the node in place")
  void upcastersThatChangeTheNodeInPlaceGiveTheSameResult() {
    Event event = read(storedAtRevision1(), new ChangingInPlace());

    assertThat(event.getRevision()).isEqualTo(3);
    assertThat(event.getPayload()).isEqualTo(Renamed.builder().id("ada").fullName("Ada Lovelace").country("NL").build());
  }

  @Test
  @DisplayName("Should only apply the upcasters from the revision an event was stored at")
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
  @DisplayName("Should stop the chain at its revision when an upcaster returns null")
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
  @DisplayName("Should fail when an upcaster does not return a JSON object")
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
  @DisplayName("Should refuse two upcasters for the same type and revision in a serde")
  void twoUpcastersForTheSameTypeAndRevisionAreRefusedBySerde() {
    assertThatThrownBy(() -> new EventSerde().registerUpcaster(new TwoForRevision1()))
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("Two upcasters for " + TYPE + " revision 1");
  }

  @Test
  @DisplayName("Should refuse two upcasters for the same type and revision in Eventify")
  void twoUpcastersForTheSameTypeAndRevisionAreRefusedByEventify() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "upcasting-chain-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    assertThatThrownBy(() -> Eventify.builder().streamsConfig(properties).registerHandler(new TwoForRevision1()).build())
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("Two upcasters for " + TYPE + " revision 1");
  }

  /** The name {@link Client} had at revision 1: a class that no longer exists. */
  private static final String CUSTOMER = "io.github.alikelleci.eventify.core.upcasting.UpcastingChainTest$Customer";
  private static final String CLIENT = "io.github.alikelleci.eventify.core.upcasting.UpcastingChainTest$Client";

  /** Revision 1: named {@code Customer}, with {@code name}. Revision 2: {@code Client}, with {@code fullName}. Revision 3: {@code country}. */
  @Value
  @Builder
  @Revision(3)
  public static class Client {
    @AggregateId
    String id;
    String fullName;
    String country;
  }

  public static class RenamingTheClass {
    /** Revision 1 → 2: {@code Customer} becomes {@code Client}, and {@code name} becomes {@code fullName}. */
    @Upcast(type = CUSTOMER, revision = 1)
    public JsonNode renameToClient(ObjectNode payload) {
      payload.put("@class", CLIENT);
      payload.set("fullName", payload.remove("name"));
      return payload;
    }

    /** Revision 2 → 3, registered for the new class. */
    @Upcast(type = CLIENT, revision = 2)
    public JsonNode addCountry(ObjectNode payload) {
      payload.put("country", "NL");
      return payload;
    }
  }

  @Test
  @DisplayName("Should read an event under its new class when an upcaster renames the class, and go on with that class's upcasters")
  void anUpcasterRenamesTheEventClass() {
    String stored = storedAtRevision1()
        .replace(TYPE, CUSTOMER)
        .replace("\"type\":\"Renamed\"", "\"type\":\"Customer\"");

    Event event = read(stored, new RenamingTheClass());

    assertThat(event.getRevision()).isEqualTo(3);
    assertThat(event.getType()).isEqualTo("Client");
    assertThat(event.getPayload()).isEqualTo(Client.builder().id("ada").fullName("Ada Lovelace").country("NL").build());
  }

  /** Builds its node from scratch, without "@class". */
  public static class BuildingANodeWithoutClass {
    @Upcast(type = TYPE, revision = 1)
    public JsonNode fromScratch(ObjectNode payload) {
      ObjectNode upcasted = JsonNodeFactory.instance.objectNode();
      upcasted.put("id", payload.path("id").asText());
      upcasted.put("fullName", payload.path("name").asText());
      return upcasted;
    }
  }

  @Test
  @DisplayName("Should keep the class when an upcaster returns a node without @class")
  void anUpcasterWithoutClassKeepsTheClass() {
    Event event = read(storedAtRevision1(), new BuildingANodeWithoutClass());

    assertThat(event.getRevision()).isEqualTo(2);
    assertThat(event.getType()).isEqualTo("Renamed");
    assertThat(event.getPayload()).isEqualTo(Renamed.builder().id("ada").fullName("Ada Lovelace").build());
  }

  /** The event as it was stored at revision 1: with {@code name}, before it was renamed. */
  private static String storedAtRevision1() {
    Event event = Event.builder().aggregateType("profile").payload(Renamed.builder().id("ada").name("Ada Lovelace").build()).sequence(1).build();
    String json = new String(new JsonSerializer<Event>(EventifyObjectMapper.create()).serialize("events", event));
    return json.replace("\"revision\":3", "\"revision\":1");
  }

  private static Event read(String stored, Object upcasters) {
    return new EventSerde()
        .registerUpcaster(upcasters)
        .deserializer()
        .deserialize("events", stored.getBytes());
  }
}
