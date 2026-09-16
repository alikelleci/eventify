package io.github.alikelleci.eventify.console.plugin;

import io.github.alikelleci.eventify.console.plugin.EventifyService.ApiResult;
import io.github.alikelleci.eventify.console.plugin.item.ItemCommand.CreateItem;
import io.github.alikelleci.eventify.console.plugin.item.ItemHandler;
import io.github.alikelleci.eventify.console.protocol.InstanceStatus;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.plugins.EventifyPlugin;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The status an instance reports, against a real broker: the state it is in and how long, and the state stores it
 * restores after its local state is gone.
 */
@Testcontainers
class StatusIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  @TempDir
  Path stateDir;

  private Eventify eventify;
  private EventifyService service;
  private final StatusTracker tracker = new StatusTracker();
  private final AtomicInteger restoresStarted = new AtomicInteger();

  @AfterEach
  void tearDown() {
    if (service != null) service.close();
    if (eventify != null) eventify.stop();
  }

  @BeforeAll
  static void createTopics() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic("commands.item", 2, (short) 1), new NewTopic("events.item", 2, (short) 1))).all().get();
    }
  }

  @Test
  @DisplayName("A running instance reports its state, how long it has been in it, and nothing to restore")
  void runningInstance() {
    eventify = start("status-running", "running");
    awaitRunning();

    InstanceStatus status = status();
    assertThat(status.state()).isEqualTo("RUNNING");
    assertThat(status.stateForMs()).isPositive();
    assertThat(status.restore()).isNull();
  }

  @Test
  @DisplayName("Restoring the state stores is reported, and is over once the instance runs")
  void restoresAfterLosingItsLocalState() throws Exception {
    eventify = start("status-restore", "first-run");
    awaitRunning();
    List<String> ids = IntStream.range(0, 300).mapToObj(i -> "restored-" + i).toList();
    send(ids);
    // Every command has been handled, so the stores have something to restore.
    await().atMost(Duration.ofSeconds(60)).until(() -> ids.stream().allMatch(this::hasEvents));
    eventify.stop();

    // Started again with an empty state directory: the stores are read back from their changelogs.
    restoresStarted.set(0);
    eventify = start("status-restore", "second-run");
    awaitRunning();

    assertThat(restoresStarted).hasPositiveValue();
    assertThat(status().restore()).isNull();  // restoring is over once it runs
  }

  private InstanceStatus status() {
    ApiResult<InstanceStatus> result = service.getStatus();
    assertThat(result).isInstanceOf(ApiResult.Ok.class);
    return ((ApiResult.Ok<InstanceStatus>) result).value();
  }

  private boolean hasEvents(String aggregateId) {
    return service.getEvents(aggregateId, null, 1) instanceof ApiResult.Ok<EventifyService.EventsPage> ok
        && !ok.value().events().isEmpty();
  }

  private void awaitRunning() {
    await().atMost(Duration.ofSeconds(60)).until(() -> eventify.getKafkaStreams().state() == KafkaStreams.State.RUNNING);
  }

  private void send(List<String> ids) throws Exception {
    try (KafkaProducer<String, Command> producer = new KafkaProducer<>(
        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()), new StringSerializer(), new JsonSerializer<>())) {
      for (String id : ids) {
        Command command = Command.builder().payload(CreateItem.builder().id(id).name("Item " + id).build()).build();
        producer.send(new ProducerRecord<>("commands.item", id, command));
      }
      producer.flush();
    }
  }

  /** Wired like the console plugin does it: the tracker is told through the plugin hooks. */
  private Eventify start(String applicationId, String name) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.resolve(name).toString());
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

    Eventify instance = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new ItemHandler())
        .registerPlugin(new EventifyPlugin() {
          @Override
          public StateListener stateListener() {
            return tracker;
          }

          @Override
          public StateRestoreListener stateRestoreListener() {
            return new StateRestoreListener() {
              @Override
              public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
                restoresStarted.incrementAndGet();
                tracker.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
              }

              @Override
              public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
                tracker.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
              }

              @Override
              public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
                tracker.onRestoreEnd(topicPartition, storeName, totalRestored);
              }

              @Override
              public void onRestoreSuspended(TopicPartition topicPartition, String storeName, long totalRestored) {
                tracker.onRestoreSuspended(topicPartition, storeName, totalRestored);
              }
            };
          }
        })
        .build();
    instance.start();
    if (service != null) service.close();
    service = new EventifyService(instance, tracker);
    return instance;
  }
}
