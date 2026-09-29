package io.github.alikelleci.eventify.migration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Reads a changelog one partition at a time, without a consumer group, so it commits nothing and can read while the
 * application runs. A compacted topic still holds older values of a key; only the last record of a key counts, and a
 * key whose last record is a tombstone does not exist.
 */
final class ChangelogReader implements AutoCloseable {

  private final KafkaConsumer<String, byte[]> consumer;

  ChangelogReader(Properties config) {
    Properties properties = new Properties();
    properties.putAll(config);
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false); // asking for a missing topic must not create it
    properties.remove(ConsumerConfig.GROUP_ID_CONFIG);
    this.consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
  }

  /** The partition numbers of a topic, or an empty list when the topic does not exist. */
  List<Integer> partitions(String topic) {
    List<PartitionInfo> partitions = consumer.partitionsFor(topic);
    return partitions == null ? List.of() : partitions.stream().map(PartitionInfo::partition).sorted().toList();
  }

  /** Where a partition ends now: everything committed before it is read, anything written later is not. */
  long end(TopicPartition partition) {
    return consumer.endOffsets(List.of(partition)).get(partition);
  }

  /** The offset of the last record of every key that exists, up to {@code end}. */
  Map<String, Long> liveOffsets(TopicPartition partition, long end) {
    Map<String, Long> offsets = new HashMap<>();
    read(partition, end, record -> {
      if (record.value() == null) {
        offsets.remove(record.key());
      } else {
        offsets.put(record.key(), record.offset());
      }
    });
    return offsets;
  }

  /** Reads the partition again and gives the key and value of each record that is the last one of its key. */
  void forEachLive(TopicPartition partition, long end, Map<String, Long> liveOffsets, BiConsumer<String, byte[]> action) {
    read(partition, end, record -> {
      Long offset = liveOffsets.get(record.key());
      if (offset != null && offset == record.offset()) {
        action.accept(record.key(), record.value());
      }
    });
  }

  private void read(TopicPartition partition, long end, Consumer<ConsumerRecord<String, byte[]>> action) {
    consumer.assign(List.of(partition));
    consumer.seekToBeginning(List.of(partition));
    while (consumer.position(partition) < end) {
      for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofSeconds(1))) {
        if (record.offset() < end && record.key() != null) {
          action.accept(record);
        }
      }
    }
  }

  @Override
  public void close() {
    consumer.close();
  }
}
