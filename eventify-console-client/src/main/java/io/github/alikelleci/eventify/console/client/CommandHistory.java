package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.client.ConsoleViews.CommandView;
import io.github.alikelleci.eventify.console.client.ConsoleViews.CommandsPage;
import io.github.alikelleci.eventify.console.client.ConsoleViews.Result;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.kafka.TopicNames;
import io.github.alikelleci.eventify.console.protocol.Requests;
import io.github.alikelleci.eventify.core.plugin.PluginContext;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** The commands of an aggregate, read from the result topics: every handled command is written there. */
@Slf4j
class CommandHistory {

  /** How far back the commands are read. */
  private static final Duration COMMANDS_LOOKBACK = Duration.ofDays(7);
  /** Reading the commands stops after this long, so a read that can't finish doesn't keep a query thread. */
  private static final Duration MAX_COMMANDS_READ = Duration.ofSeconds(60);

  private final PluginContext eventify;
  private final ObjectMapper objectMapper;

  CommandHistory(PluginContext eventify) {
    this.eventify = eventify;
    this.objectMapper = eventify.getObjectMapper();
  }

  /** The application's own Kafka client settings (security included, and its {@code consumer.} settings), to read a topic without a group. */
  static Map<String, Object> consumerConfig(PluginContext eventify) {
    Map<String, Object> config = new StreamsConfig(eventify.getStreamsConfig()).getRestoreConsumerConfigs(NodeIdentity.clientId(eventify, "commands"));
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    // Every read seeks to where it starts; this only applies when that offset is no longer there.
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    return config;
  }

  /**
   * Reads the aggregate's commands from the result topics. Every call has its own consumer, so calls never affect each
   * other. When the request is cancelled, only this call's consumer stops, the way Kafka intends: with a wakeup.
   */
  Result<CommandsPage> read(Requests.Commands request, CancelSignal cancel) {
    String aggregateType = request.aggregateType();
    String aggregateId = request.aggregateId();
    int limit = request.limit();
    // Eventify writes the result of every handled command to a result topic of its own.
    Set<String> resultTopics = eventify.getCommandTopics(aggregateType).stream()
        .map(TopicNames::resultTopicOf)
        .collect(Collectors.toSet());
    if (resultTopics.isEmpty()) {
      return Result.ok(new CommandsPage(List.of(), COMMANDS_LOOKBACK.toDays(), false));
    }

    List<CommandView> results = new ArrayList<>();
    JsonDeserializer<CommandResult> resultDeserializer = new JsonDeserializer<>(CommandResult.class, objectMapper);
    Instant deadline = Instant.now().plus(MAX_COMMANDS_READ);

    // Closed in reverse order: the wakeup is unregistered before the consumer closes.
    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig(eventify), new StringDeserializer(), new ByteArrayDeserializer());
         AutoCloseable stopOnCancel = cancel.onCancel(consumer::wakeup)) {
      for (String topic : resultTopics) {
        try {
          readCommands(consumer, topic, aggregateId, resultDeserializer, deadline, results);
        } catch (WakeupException e) {
          throw e;
        } catch (CommandsReadTimeout e) {
          log.warn("Reading the commands of aggregate {} {} took longer than {}", aggregateType, aggregateId, MAX_COMMANDS_READ);
          return Result.unavailable("Reading the commands took too long");
        } catch (Exception e) {
          // Not an empty list: that would look like the aggregate has no commands.
          log.warn("Failed to read commands of aggregate {} {} from topic {}", aggregateType, aggregateId, topic, e);
          return Result.unavailable("Failed to read commands from topic " + topic + ": " + e.getMessage());
        }
      }
    } catch (WakeupException e) {
      log.debug("Stopped reading commands for aggregate {} {}: the request was cancelled", aggregateType, aggregateId);
      return Result.unavailable("Cancelled");
    } catch (Exception e) {
      log.error("Unexpected error querying commands for aggregate {} {}", aggregateType, aggregateId, e);
      return Result.unavailable("Unexpected error");
    }

    results.sort((a, b) -> b.command().getTimestamp().compareTo(a.command().getTimestamp()));
    boolean truncated = results.size() > limit;
    List<CommandView> limited = truncated ? results.subList(0, limit) : results;
    return Result.ok(new CommandsPage(limited, COMMANDS_LOOKBACK.toDays(), truncated));
  }

  /**
   * Adds the aggregate's commands of the last {@link #COMMANDS_LOOKBACK} from one result topic: from the partition its
   * key is written to, up to the end of that partition when the read starts.
   */
  private static void readCommands(KafkaConsumer<String, byte[]> consumer, String topic, String aggregateId,
                                   JsonDeserializer<CommandResult> resultDeserializer, Instant deadline, List<CommandView> results) {
    int numPartitions = consumer.partitionsFor(topic).size();
    if (numPartitions == 0) {
      return;
    }
    // The partition the default partitioner picks for this key, as Eventify writes the results.
    int partition = Utils.toPositive(Utils.murmur2(aggregateId.getBytes(StandardCharsets.UTF_8))) % numPartitions;
    TopicPartition tp = new TopicPartition(topic, partition);
    consumer.assign(List.of(tp));

    long endOffset = consumer.endOffsets(List.of(tp)).getOrDefault(tp, 0L);
    long since = Instant.now().minus(COMMANDS_LOOKBACK).toEpochMilli();
    OffsetAndTimestamp first = consumer.offsetsForTimes(Map.of(tp, since)).get(tp);
    // None: every record is older than the lookback, so there is nothing to read.
    long startOffset = first != null ? first.offset() : endOffset;
    if (startOffset >= endOffset) {
      return;
    }
    consumer.seek(tp, startOffset);

    // Until the position passes the end, not until a poll comes back empty: a slow poll isn't the end, and the last
    // offsets can be transaction markers, which are never returned as records.
    while (consumer.position(tp) < endOffset) {
      if (Instant.now().isAfter(deadline)) {
        throw new CommandsReadTimeout();
      }
      for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofSeconds(1))) {
        if (record.offset() >= endOffset || !aggregateId.equals(record.key()) || record.value() == null) {
          continue;
        }
        try {
          CommandResult result = resultDeserializer.deserialize(topic, record.value());
          if (result != null && result.command() != null) {
            results.add(CommandView.of(result));
          }
        } catch (Exception e) {
          log.warn("Failed to deserialize command record on topic {} at offset {}", topic, record.offset(), e);
        }
      }
    }
  }

  private static class CommandsReadTimeout extends RuntimeException {
  }
}
