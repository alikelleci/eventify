package io.github.alikelleci.eventify.console.plugin;

import io.github.alikelleci.eventify.console.plugin.EventifyService.Result;
import io.github.alikelleci.eventify.console.plugin.item.ItemCommand.CreateItem;
import io.github.alikelleci.eventify.console.plugin.item.ItemHandler;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Two instances of one application on a real broker. The instances have no address, only the name Eventify puts in
 * {@code application.server}; an instance that doesn't own an aggregate must name the one that does.
 */
@Testcontainers
class EventifyServiceRoutingIT {

  private static final String APPLICATION_ID = "routing-test";

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  @TempDir
  Path stateDir;

  private Eventify first;
  private Eventify second;

  @AfterEach
  void tearDown() {
    if (first != null) first.stop();
    if (second != null) second.stop();
  }

  @BeforeAll
  static void createTopics() throws Exception {
    // Co-partitioned source topics, with a partition for each instance.
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic("commands.item", 2, (short) 1), new NewTopic("events.item", 2, (short) 1))).all().get();
    }
  }

  @Test
  void anInstanceThatDoesNotOwnAnAggregateNamesTheOwner() throws Exception {
    first = start(APPLICATION_ID, "first");
    second = start(APPLICATION_ID, "second");
    assertThat(first.getStreamsConfig().getProperty(StreamsConfig.APPLICATION_SERVER_CONFIG))
        .matches(APPLICATION_ID + "\\.[0-9a-f-]{36}:0");

    await().atMost(Duration.ofSeconds(60)).until(() ->
        first.getKafkaStreams().state() == KafkaStreams.State.RUNNING
            && second.getKafkaStreams().state() == KafkaStreams.State.RUNNING);

    List<String> aggregateIds = IntStream.range(0, 20).mapToObj(i -> "item-" + i).toList();
    try (KafkaProducer<String, Command> producer = new KafkaProducer<>(
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringSerializer(), new JsonSerializer<>())) {
      for (String id : aggregateIds) {
        Command command = Command.builder().payload(CreateItem.builder().id(id).name("Item " + id).build()).build();
        producer.send(new ProducerRecord<>("commands.item", id, command)).get();
      }
    }

    EventifyService firstService = new EventifyService(first, new StatusTracker());
    EventifyService secondService = new EventifyService(second, new StatusTracker());
    String firstId = EventifyService.nodeId(EventifyService.hostInfo(first));
    String secondId = EventifyService.nodeId(EventifyService.hostInfo(second));
    Set<String> owners = new HashSet<>();

    try {
      for (String id : aggregateIds) {
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
          Result<EventifyService.EventsPage> fromFirst = firstService.getEvents(id, null, 50);
          Result<EventifyService.EventsPage> fromSecond = secondService.getEvents(id, null, 50);

          // Exactly one answers with the event; the other names it as the owner.
          if (fromFirst.isOk()) {
            assertThat(fromFirst.value().events()).hasSize(1);
            assertThat(fromSecond).isEqualTo(Result.notOwner(firstId));
            owners.add(firstId);
          } else {
            assertThat(fromSecond.isOk()).isTrue();
            assertThat(fromSecond.value().events()).hasSize(1);
            assertThat(fromFirst).isEqualTo(Result.notOwner(secondId));
            owners.add(secondId);
          }
        });
      }
    } finally {
      firstService.close();
      secondService.close();
    }

    // With two partitions, both instances own some of the aggregates.
    assertThat(owners).containsExactlyInAnyOrder(firstId, secondId);
  }

  /** An application with only a command handler and an event sourcing handler shows its commands. */
  @Test
  void theCommandsAreReadAndACancelledReadStops() throws Exception {
    first = start("commands-test", "commands");
    await().atMost(Duration.ofSeconds(60)).until(() -> first.getKafkaStreams().state() == KafkaStreams.State.RUNNING);

    String id = "item-with-commands";
    try (KafkaProducer<String, Command> producer = new KafkaProducer<>(
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringSerializer(), new JsonSerializer<>())) {
      Command command = Command.builder().payload(CreateItem.builder().id(id).name("Item").build()).build();
      producer.send(new ProducerRecord<>("commands.item", id, command)).get();
    }

    EventifyService service = new EventifyService(first, new StatusTracker());
    try {
      await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
          assertThat(service.getCommands(id, 50, new CancelSignal()).value())
              .satisfies(page -> assertThat(page.commands()).isNotEmpty()));

      // Someone refreshed the page: the read stops instead of reading the topic.
      CancelSignal refreshed = new CancelSignal();
      refreshed.cancel();
      assertThat(service.getCommands(id, 50, refreshed)).isEqualTo(Result.unavailable("Cancelled"));
    } finally {
      service.close();
    }
  }

  private Eventify start(String applicationId, String name) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.resolve(name).toString());
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

    Eventify eventify = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new ItemHandler())
        .build();
    eventify.start();
    return eventify;
  }
}
