package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.client.ConsoleViews.Result;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.plugin.PluginContext;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.REPLY_TO;

/** Sends a command again, as the console asks: as a new command, on the topic of the command it retries. */
@Slf4j
class CommandRetry {

  /** On a command retried from the console: the id of the command it retries. */
  static final String RETRY_OF = "$retryOf";

  /** How long a retried command may take to reach Kafka before the console is told it failed. */
  private static final Duration RETRY_SEND_TIMEOUT = Duration.ofSeconds(10);

  private final PluginContext eventify;
  private final ObjectMapper objectMapper;
  private final Producer<String, Command> producer;

  CommandRetry(PluginContext eventify) {
    this.eventify = eventify;
    this.objectMapper = eventify.getObjectMapper();
    this.producer = new KafkaProducer<>(producerConfig(eventify), new StringSerializer(), new JsonSerializer<>(objectMapper));
  }

  void close() {
    producer.close();
  }

  /**
   * The application's own Kafka client settings (security included, and its {@code producer.} settings), without the
   * ones Kafka Streams only adds for its exactly-once processing.
   */
  static Map<String, Object> producerConfig(PluginContext eventify) {
    Map<String, Object> config = new StreamsConfig(eventify.getStreamsConfig()).getProducerConfigs(NodeIdentity.clientId(eventify, "producer"));
    config.remove(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
    config.remove(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
    config.remove(ProducerConfig.LINGER_MS_CONFIG);
    return config;
  }

  /**
   * Sends the command again, as a new command: with its own id and correlation id, so the events it produces are its
   * own, and {@link #RETRY_OF} pointing to the command it retries.
   *
   * @param json the command as the console received it. Only a command this application handles is accepted, checked
   *             before it is read: the JSON names the class to create.
   */
  Result<Void> retry(byte[] json) {
    Command original;
    try {
      JsonNode tree = objectMapper.readTree(json);
      String type = tree.path("payload").path("@class").asText(null);
      boolean handled = type != null && eventify.getCommandTypes().stream()
          .anyMatch(commandClass -> commandClass.getName().equals(type));
      if (!handled) {
        return Result.badRequest("Not a command of this application: " + type);
      }
      original = objectMapper.treeToValue(tree, Command.class);
    } catch (Exception e) {
      log.warn("Failed to read the command to retry", e);
      return Result.badRequest("Unreadable command");
    }

    // The correlation id stays: the retry belongs to the same flow (e.g. a saga) as the command it retries, and its
    // events can be traced with the rest of that flow. RETRY_OF tells the retry apart from the original.
    Metadata retryMetadata = Metadata.builder()
        .putAll(original.getMetadata())
        .put(RETRY_OF, original.getId())
        .build();
    retryMetadata.remove(REPLY_TO);

    Command retryCommand = Command.builder()
        .payload(original.getPayload())
        .metadata(retryMetadata)
        .build();

    String commandTopic = original.getTopic().value();

    // Waits until Kafka has the command: only then is the retry done. A send that fails later (no access to the topic,
    // the broker unreachable) would otherwise be reported as done.
    try {
      producer.send(new ProducerRecord<>(commandTopic, null, retryCommand.getTimestamp().toEpochMilli(),
          retryCommand.getAggregateId(), retryCommand)).get(RETRY_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      log.info("Retried command {} as {} on topic {}", original.getId(), retryCommand.getId(), commandTopic);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Result.unavailable("Interrupted while publishing the retry command");
    } catch (Exception e) {
      Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
      log.error("Failed to publish retry command for {}", original.getId(), cause);
      return Result.unavailable("Failed to publish retry command: " + cause.getMessage());
    }

    return Result.ok(null);
  }
}
