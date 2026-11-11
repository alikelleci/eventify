package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.aggregate.AggregateRepository;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.SnapshotStore;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventStore;
import io.github.alikelleci.eventify.core.internal.StoreKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import jakarta.validation.ValidationException;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Random commands against two aggregate types, one that keeps its events and one that deletes them at a snapshot.
 * After every command the state, the snapshot, the stored events and the state at every earlier version are compared
 * with a model kept in the test.
 */
@DisplayName("Event sourcing against a model")
class EventSourcingModelTest {

  /** A snapshot every 3 events; keeps its events. */
  @Value
  @Builder(toBuilder = true)
  @AggregateRoot("ledger")
  @EnableSnapshotting(threshold = 3)
  public static class Ledger {
    @AggregateId
    String id;
    int count;
    long digest;
  }

  @Topic("commands.ledger")
  public interface LedgerCommand {
  }

  @Topic("events.ledger")
  public interface LedgerEvent {
  }

  @Value
  @Builder
  public static class OpenLedger implements LedgerCommand {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class AddLedger implements LedgerCommand {
    @AggregateId
    String id;
    List<Integer> values;
  }

  @Value
  @Builder
  public static class CloseLedger implements LedgerCommand {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class LedgerOpened implements LedgerEvent {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class LedgerAdded implements LedgerEvent {
    @AggregateId
    String id;
    int value;
  }

  @Value
  @Builder
  public static class LedgerClosed implements LedgerEvent {
    @AggregateId
    String id;
  }

  /** A snapshot every 2 events; deletes the events before it. */
  @Value
  @Builder(toBuilder = true)
  @AggregateRoot("vault")
  @EnableSnapshotting(threshold = 2, deleteEvents = true)
  public static class Vault {
    @AggregateId
    String id;
    int count;
    long digest;
  }

  @Topic("commands.vault")
  public interface VaultCommand {
  }

  @Topic("events.vault")
  public interface VaultEvent {
  }

  @Value
  @Builder
  public static class OpenVault implements VaultCommand {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class AddVault implements VaultCommand {
    @AggregateId
    String id;
    List<Integer> values;
  }

  @Value
  @Builder
  public static class CloseVault implements VaultCommand {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class VaultOpened implements VaultEvent {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class VaultAdded implements VaultEvent {
    @AggregateId
    String id;
    int value;
  }

  @Value
  @Builder
  public static class VaultClosed implements VaultEvent {
    @AggregateId
    String id;
  }

  public static class Handlers {
    @CommandHandler
    public LedgerEvent handle(OpenLedger c, Ledger s) {
      if (s != null) throw new ValidationException("exists");
      return LedgerOpened.builder().id(c.getId()).build();
    }

    @CommandHandler
    public List<LedgerEvent> handle(AddLedger c, Ledger s) {
      if (s == null) throw new ValidationException("missing");
      return c.getValues().stream().map(v -> (LedgerEvent) LedgerAdded.builder().id(c.getId()).value(v).build()).toList();
    }

    @CommandHandler
    public LedgerEvent handle(CloseLedger c, Ledger s) {
      if (s == null) throw new ValidationException("missing");
      return LedgerClosed.builder().id(c.getId()).build();
    }

    @EventSourcingHandler
    public Ledger on(LedgerOpened e, Ledger s) {
      return Ledger.builder().id(e.getId()).build();
    }

    @EventSourcingHandler
    public Ledger on(LedgerAdded e, Ledger s) {
      return s.toBuilder().count(s.getCount() + 1).digest(s.getDigest() * 31 + e.getValue()).build();
    }

    @EventSourcingHandler
    public Ledger on(LedgerClosed e, Ledger s) {
      return null;
    }

    @CommandHandler
    public VaultEvent handle(OpenVault c, Vault s) {
      if (s != null) throw new ValidationException("exists");
      return VaultOpened.builder().id(c.getId()).build();
    }

    @CommandHandler
    public List<VaultEvent> handle(AddVault c, Vault s) {
      if (s == null) throw new ValidationException("missing");
      return c.getValues().stream().map(v -> (VaultEvent) VaultAdded.builder().id(c.getId()).value(v).build()).toList();
    }

    @CommandHandler
    public VaultEvent handle(CloseVault c, Vault s) {
      if (s == null) throw new ValidationException("missing");
      return VaultClosed.builder().id(c.getId()).build();
    }

    @EventSourcingHandler
    public Vault on(VaultOpened e, Vault s) {
      return Vault.builder().id(e.getId()).build();
    }

    @EventSourcingHandler
    public Vault on(VaultAdded e, Vault s) {
      return s.toBuilder().count(s.getCount() + 1).digest(s.getDigest() * 31 + e.getValue()).build();
    }

    @EventSourcingHandler
    public Vault on(VaultClosed e, Vault s) {
      return null;
    }
  }

  /** Ids that are prefixes of each other, differ in digits and length, or hold odd characters; shared by both types. */
  private static final List<String> IDS = List.of("a", "a-1", "a-10", "a1", "a\u0001", "a 0000000000000000001", "ä", "A", "9", "10");

  /** The model: per aggregate its events, as "O", "C" or a value. */
  private final Map<String, List<Object>> model = new HashMap<>();
  private long accepted;

  @Test
  @DisplayName("Should give the state, snapshot, stored events and every earlier state the model expects, after every command")
  void randomCommandsKeepEveryInvariant() {
    for (int seed = 1; seed <= 25; seed++) {
      run(seed, 400);
    }
    assertThat(accepted).as("accepted commands: the test must not pass on rejections alone").isGreaterThan(5000);
  }

  private void run(int seed, int commandCount) {
    model.clear();
    Random random = new Random(seed);
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-sourcing-model-" + seed);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    Eventify eventify = Eventify.builder().streamsConfig(properties).registerHandler(new Handlers()).build();
    try (TopologyTestDriver driver = new TopologyTestDriver(eventify.topology())) {
      Map<String, TestInputTopic<String, Command>> in = Map.of(
          "ledger", driver.createInputTopic("commands.ledger", new StringSerializer(), new JsonSerializer<>()),
          "vault", driver.createInputTopic("commands.vault", new StringSerializer(), new JsonSerializer<>()));
      Map<String, TestOutputTopic<String, CommandResult>> results = Map.of(
          "ledger", driver.createOutputTopic("commands.ledger.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class)),
          "vault", driver.createOutputTopic("commands.vault.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class)));
      Map<String, TestOutputTopic<String, Event>> out = Map.of(
          "ledger", driver.createOutputTopic("events.ledger", new StringDeserializer(), new JsonDeserializer<>(Event.class)),
          "vault", driver.createOutputTopic("events.vault", new StringDeserializer(), new JsonDeserializer<>(Event.class)));
      KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
      KeyValueStore<String, AggregateState> snapshotStore = driver.getKeyValueStore("snapshot-store");

      for (int i = 0; i < commandCount; i++) {
        String type = random.nextBoolean() ? "ledger" : "vault";
        String id = IDS.get(random.nextInt(IDS.size()));
        List<Object> history = model.computeIfAbsent(type + "/" + id, key -> new ArrayList<>());
        Object before = fold(type, id, history, history.size());
        // Mostly open what doesn't exist and rarely close, so histories grow long.
        int kind = before == null ? (random.nextInt(10) < 8 ? 0 : 2) : (random.nextInt(40) == 0 ? 1 : random.nextInt(20) == 0 ? 0 : 2);
        Object payload;
        List<Object> expected = new ArrayList<>();
        boolean accept;
        if (kind == 0) {
          payload = type.equals("ledger") ? OpenLedger.builder().id(id).build() : OpenVault.builder().id(id).build();
          accept = before == null;
          expected.add("O");
        } else if (kind == 1) {
          payload = type.equals("ledger") ? CloseLedger.builder().id(id).build() : CloseVault.builder().id(id).build();
          accept = before != null;
          expected.add("C");
        } else {
          List<Integer> values = new ArrayList<>();
          for (int n = random.nextInt(5); n > 0; n--) values.add(random.nextInt(1000));
          payload = type.equals("ledger") ? AddLedger.builder().id(id).values(values).build() : AddVault.builder().id(id).values(values).build();
          accept = before != null;
          expected.addAll(values);
        }
        Command command = Command.builder().payload(payload).build();
        in.get(type).pipeInput(command.getAggregateId(), command);

        CommandResult result = results.get(type).readValue();
        assertThat(results.get(type).isEmpty()).isTrue();
        List<Event> sent = out.get(type).readValuesToList();
        if (!accept) {
          assertThat(result).isInstanceOf(CommandResult.Failure.class);
          assertThat(sent).isEmpty();
        } else {
          accepted++;
          assertThat(result).isInstanceOf(CommandResult.Success.class);
          List<Event> resultEvents = ((CommandResult.Success) result).events();
          assertThat(resultEvents).extracting(Event::getId).containsExactlyElementsOf(sent.stream().map(Event::getId).toList());
          assertThat(sent).hasSize(expected.size());
          for (int n = 0; n < sent.size(); n++) {
            assertThat(sent.get(n).getSequence()).isEqualTo(history.size() + n + 1);
            assertThat(sent.get(n).getAggregateType()).isEqualTo(type);
            assertThat(sent.get(n).getAggregateId()).isEqualTo(id);
          }
          history.addAll(expected);
        }
        check(eventify, type, id, history, eventStore, snapshotStore, random);
      }
      // At the end every aggregate again, and the stores hold nothing else.
      long expectedKeys = 0;
      for (Map.Entry<String, List<Object>> entry : model.entrySet()) {
        String type = entry.getKey().substring(0, entry.getKey().indexOf('/'));
        String id = entry.getKey().substring(entry.getKey().indexOf('/') + 1);
        expectedKeys += check(eventify, type, id, entry.getValue(), eventStore, snapshotStore, random);
      }
      long keys = 0;
      try (KeyValueIterator<String, Event> all = eventStore.all()) {
        while (all.hasNext()) { all.next(); keys++; }
      }
      assertThat(keys).isEqualTo(expectedKeys);
    }
  }

  /** Checks one aggregate against the model; returns how many events it has in the store. */
  private long check(Eventify eventify, String type, String id, List<Object> history, KeyValueStore<String, Event> eventStore,
                     KeyValueStore<String, AggregateState> snapshotStore, Random random) {
    AggregateRepository repository = eventify.getAggregateRepository(type, new EventStore(eventStore), new SnapshotStore(snapshotStore));
    int version = history.size();

    // Current state: snapshot + remaining events equals the model's fold of the full history.
    AggregateState current = repository.replay(id).currentState();
    assertThat(current.getVersion()).isEqualTo(version);
    assertThat(current.getPayload()).isEqualTo(fold(type, id, history, version));

    // Snapshot: never ahead of the events, at the last threshold multiple that a command reached or stepped over.
    AggregateState snapshot = snapshotStore.get(StoreKeys.aggregate(type, id));
    long snapshotVersion = snapshot != null ? snapshot.getVersion() : 0;
    int threshold = type.equals("ledger") ? 3 : 2;
    assertThat(snapshotVersion).isLessThanOrEqualTo(version);
    assertThat(snapshotVersion / threshold).isEqualTo(version / threshold);
    if (snapshot != null) {
      assertThat(snapshot.getPayload()).isEqualTo(fold(type, id, history, (int) snapshotVersion));
    }

    // Stored events: exactly the expected sequences, each the right event.
    long first = type.equals("vault") && snapshotVersion > 0 ? snapshotVersion : 1;
    List<Long> stored = new ArrayList<>();
    try (EventStore.EventIterator events = repository.events(id)) {
      while (events.hasNext()) {
        Event event = events.next();
        assertThat(event.getAggregateId()).isEqualTo(id);
        assertThat(event.getAggregateType()).isEqualTo(type);
        stored.add(event.getSequence());
      }
    }
    List<Long> expectedStored = new ArrayList<>();
    for (long sequence = first; sequence <= version; sequence++) expectedStored.add(sequence);
    assertThat(stored).isEqualTo(expectedStored);

    // Newest first, a random range: the same events, reversed.
    if (version >= first && version > 0) {
      long low = first + random.nextInt((int) (version - first + 1));
      long high = low + random.nextInt((int) (version - low + 1));
      List<Long> reversed = new ArrayList<>();
      try (EventStore.EventIterator events = repository.eventsNewestFirst(id, high, low)) {
        while (events.hasNext()) reversed.add(events.next().getSequence());
      }
      List<Long> expectedReversed = new ArrayList<>();
      for (long sequence = high; sequence >= low; sequence--) expectedReversed.add(sequence);
      assertThat(reversed).isEqualTo(expectedReversed);
    }

    // Point in time: every version, also past the end.
    for (int at = 0; at <= version + 2; at++) {
      AggregateState state = repository.stateAt(id, at);
      int expectedVersion = Math.min(at, version);
      boolean reachable = type.equals("ledger") || at >= snapshotVersion || snapshotVersion <= 1;
      if (!reachable) {
        assertThat(state).as("%s %s at %s, snapshot %s", type, id, at, snapshotVersion).isNull();
        continue;
      }
      assertThat(state).as("%s %s at %s, snapshot %s", type, id, at, snapshotVersion).isNotNull();
      assertThat(state.getVersion()).isEqualTo(expectedVersion);
      assertThat(state.getPayload()).isEqualTo(fold(type, id, history, expectedVersion));
      }
    return stored.size();
  }

  /** The model's own fold of the first {@code upTo} events. */
  private static Object fold(String type, String id, List<Object> history, int upTo) {
    boolean exists = false;
    int count = 0;
    long digest = 0;
    for (int i = 0; i < upTo; i++) {
      Object event = history.get(i);
      if ("O".equals(event)) { exists = true; count = 0; digest = 0; }
      else if ("C".equals(event)) { exists = false; }
      else { count++; digest = digest * 31 + (Integer) event; }
    }
    if (!exists) return null;
    return type.equals("ledger") ? Ledger.builder().id(id).count(count).digest(digest).build() : Vault.builder().id(id).count(count).digest(digest).build();
  }
}
