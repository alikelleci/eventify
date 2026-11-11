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
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcaster;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
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
    @Upcaster(type = TYPE, revision = 1)
    public JsonNode nameToFullName(JsonNode payload) {
      ObjectNode upcasted = payload.deepCopy();
      upcasted.set("fullName", upcasted.remove("name"));
      return upcasted;
    }

    @Upcaster(type = TYPE, revision = 2)
    public JsonNode addCountry(JsonNode payload) {
      ObjectNode upcasted = payload.deepCopy();
      upcasted.put("country", "NL");
      return upcasted;
    }
  }

  /** Each upcaster changes the node it was given, and returns it. */
  public static class ChangingInPlace {
    @Upcaster(type = TYPE, revision = 1)
    public JsonNode nameToFullName(ObjectNode payload) {
      payload.set("fullName", payload.remove("name"));
      return payload;
    }

    @Upcaster(type = TYPE, revision = 2)
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
  @DisplayName("Should leave the serde withUpcasters was called on unchanged")
  void withUpcastersMakesANewSerde() {
    EventSerde plain = new EventSerde();
    plain.withUpcasters(new ReturningNewNodes());

    Event event = plain.deserializer().deserialize("events", storedAtRevision1().getBytes());

    assertThat(event.getRevision()).isEqualTo(1);
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
    @Upcaster(type = TYPE, revision = 1)
    public JsonNode nameToFullName(ObjectNode payload) {
      payload.set("fullName", payload.remove("name"));
      return payload;
    }

    @Upcaster(type = TYPE, revision = 2)
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
    @Upcaster(type = TYPE, revision = 1)
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
    @Upcaster(type = TYPE, revision = 1)
    public JsonNode one(ObjectNode payload) {
      return payload;
    }

    @Upcaster(type = TYPE, revision = 1)
    public JsonNode other(ObjectNode payload) {
      return payload;
    }
  }

  @Test
  @DisplayName("Should refuse two upcasters for the same type and revision in a serde")
  void twoUpcastersForTheSameTypeAndRevisionAreRefusedBySerde() {
    assertThatThrownBy(() -> new EventSerde().withUpcasters(new TwoForRevision1()))
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("Two upcasters for " + TYPE + " revision 1");
  }

  /** As the class is written in Java source, with a dot before the nested class; its name in an event has a dollar sign. */
  public static class NamingANestedClassWithDots {
    @Upcaster(type = "io.github.alikelleci.eventify.core.upcasting.UpcastingChainTest.Renamed", revision = 1)
    public JsonNode nameToFullName(ObjectNode payload) {
      payload.set("fullName", payload.remove("name"));
      return payload;
    }
  }

  @Test
  @DisplayName("Should refuse an upcaster that names a nested class with dots: it would never be applied")
  void anUpcasterNamingANestedClassWithDotsIsRefused() {
    assertThatThrownBy(() -> new EventSerde().withUpcasters(new NamingANestedClassWithDots()))
        .isInstanceOf(HandlerRegistrationException.class)
        .hasMessageContaining("UpcastingChainTest.Renamed")
        .hasMessageContaining(TYPE);
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

  /** Was {@code Customer}, with {@code name}. Revision 1: {@code fullName}. Revision 2: {@code country}. */
  @Value
  @Builder
  @Revision(2)
  public static class Client {
    @AggregateId
    String id;
    String fullName;
    String country;
  }

  public static class RenamingTheClass {
    /** {@code Customer} becomes {@code Client} revision 1, and {@code name} becomes {@code fullName}. */
    @Upcaster(type = CUSTOMER, revision = 1)
    public JsonNode renameToClient(ObjectNode payload) {
      payload.put("@class", CLIENT);
      payload.set("fullName", payload.remove("name"));
      return payload;
    }

    /** Revision 1 → 2 of the new class: also for new {@code Client} events stored at revision 1. */
    @Upcaster(type = CLIENT, revision = 1)
    public JsonNode addCountry(ObjectNode payload) {
      payload.put("country", "NL");
      return payload;
    }
  }

  @Test
  @DisplayName("Should read an event under its new class when an upcaster renames the class, and go on from that class's revision 1")
  void anUpcasterRenamesTheEventClass() {
    String stored = storedAtRevision1()
        .replace(TYPE, CUSTOMER)
        .replace("\"type\":\"Renamed\"", "\"type\":\"Customer\"");

    Event event = read(stored, new RenamingTheClass());

    assertThat(event.getRevision()).isEqualTo(2);
    assertThat(event.getType()).isEqualTo("Client");
    assertThat(event.getPayload()).isEqualTo(Client.builder().id("ada").fullName("Ada Lovelace").country("NL").build());
  }

  public static class RenamingInACycle {
    @Upcaster(type = CUSTOMER, revision = 1)
    public JsonNode toClient(ObjectNode payload) {
      return payload.put("@class", CLIENT);
    }

    @Upcaster(type = CLIENT, revision = 1)
    public JsonNode toCustomer(ObjectNode payload) {
      return payload.put("@class", CUSTOMER);
    }
  }

  @Test
  @DisplayName("Should fail when upcasters rename a class back and forth")
  void upcastersRenamingInACycleFail() {
    String stored = storedAtRevision1()
        .replace(TYPE, CUSTOMER)
        .replace("\"type\":\"Renamed\"", "\"type\":\"Customer\"");

    assertThatThrownBy(() -> read(stored, new RenamingInACycle()))
        .isInstanceOf(SerializationException.class)
        .rootCause()
        .hasMessageContaining("in a cycle");
  }

  /** Builds its node from scratch, without "@class". */
  public static class BuildingANodeWithoutClass {
    @Upcaster(type = TYPE, revision = 1)
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

  @Value
  @Builder
  @Revision(2)
  public static class Paid {
    @AggregateId
    String id;
    BigDecimal amount;
    BigDecimal rate;
    double weight;
    Object note;
  }

  public static class LeavingPaidAsItIs {
    @Upcaster(type = "io.github.alikelleci.eventify.core.upcasting.UpcastingChainTest$Paid", revision = 1)
    public JsonNode same(ObjectNode payload) {
      return payload;
    }
  }

  @Test
  @DisplayName("Should read decimals exactly as stored, also with upcasters: not rounded, and with their zeros")
  void decimalsAreReadTheSameWithUpcasters() {
    Paid paid = Paid.builder().id("p-1").amount(new BigDecimal("10.00")).rate(new BigDecimal("12345678901234567890.123456789"))
        .weight(0.1).note(2.5).build();
    Event event = Event.builder().aggregateType("payment").payload(paid).sequence(1).build();
    String stored = new String(new JsonSerializer<Event>(EventifyObjectMapper.create()).serialize("events", event));
    Object withoutUpcasters = new EventSerde().deserializer().deserialize("events", stored.getBytes()).getPayload();
    assertThat(withoutUpcasters).isEqualTo(paid);

    // Not upcast itself: the application only has upcasters.
    assertThat(read(stored, new LeavingPaidAsItIs()).getPayload()).isEqualTo(paid);
    // Upcast from revision 1.
    Event upcast = read(stored.replace("\"revision\":2", "\"revision\":1"), new LeavingPaidAsItIs());
    assertThat(upcast.getRevision()).isEqualTo(2);
    assertThat(upcast.getPayload()).isEqualTo(paid);
  }

  /** The event as it was stored at revision 1: with {@code name}, before it was renamed. */
  private static String storedAtRevision1() {
    Event event = Event.builder().aggregateType("profile").payload(Renamed.builder().id("ada").name("Ada Lovelace").build()).sequence(1).build();
    String json = new String(new JsonSerializer<Event>(EventifyObjectMapper.create()).serialize("events", event));
    return json.replace("\"revision\":3", "\"revision\":1");
  }

  private static Event read(String stored, Object upcasters) {
    return new EventSerde()
        .withUpcasters(upcasters)
        .deserializer()
        .deserialize("events", stored.getBytes());
  }
}
