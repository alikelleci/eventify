package io.github.alikelleci.eventify.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Copies both changelogs of an Eventify 4 application before the migration, and puts them back when the application
 * has to return to Eventify 4. A copy holds the records that exist (the last one of each key), each in the partition
 * of the original, in a topic with the same settings.
 */
final class Backup {

  static final String SUFFIX = "-v4-backup";

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Properties clientConfig;
  private final String applicationId;
  private final List<String> changelogs;

  Backup(Properties clientConfig, String applicationId) {
    this.clientConfig = clientConfig;
    this.applicationId = applicationId;
    this.changelogs = List.of(applicationId + "-event-store-changelog", applicationId + "-snapshot-store-changelog");
  }

  /** Copies the changelogs, which must still be in the Eventify 4 format, to new topics; returns what it did. */
  List<String> backup() {
    List<String> done = new ArrayList<>();
    try (ChangelogReader reader = new ChangelogReader(clientConfig); Admin admin = Admin.create(clientConfig)) {
      List<String> existing = changelogs.stream().filter(topic -> !reader.partitions(topic).isEmpty()).toList();
      if (!existing.contains(changelogs.get(0))) {
        throw new IllegalStateException("Topic " + changelogs.get(0) + " does not exist. Is the application id right?");
      }
      for (String topic : existing) {
        if (!reader.partitions(topic + SUFFIX).isEmpty()) {
          throw new IllegalStateException("Topic " + topic + SUFFIX + " already exists. Restore from it, or delete it for a new backup.");
        }
        for (int number : reader.partitions(topic)) {
          TopicPartition partition = new TopicPartition(topic, number);
          if (reader.liveOffsets(partition, reader.end(partition)).keySet().stream().anyMatch(Keys::isV5)) {
            throw new IllegalStateException("Topic " + topic + " already holds Eventify 5 keys: a backup must be made before the migration.");
          }
        }
      }
      try (ChangelogWriter writer = new ChangelogWriter(clientConfig, applicationId)) {
        for (String topic : existing) {
          createLike(admin, topic, topic + SUFFIX);
          for (int number : reader.partitions(topic)) {
            TopicPartition partition = new TopicPartition(topic, number);
            long end = reader.end(partition);
            reader.forEachLive(partition, end, reader.liveOffsets(partition, end), (key, value) -> {
              writer.send(topic + SUFFIX, number, key, value);
              writer.changeWritten();
            });
          }
        }
        writer.commit();
      }
      for (String topic : existing) {
        done.add(compare(reader, topic, topic + SUFFIX, "Backed up"));
      }
    }
    return done;
  }

  /**
   * Makes the changelogs hold what their copies hold again: every record of the copy, and a tombstone for every other
   * key, which are the keys the migration wrote. Refuses when Eventify 5 recorded events of its own: they would be lost.
   */
  List<String> restore() {
    List<String> done = new ArrayList<>();
    try (ChangelogReader reader = new ChangelogReader(clientConfig)) {
      List<String> restored = changelogs.stream().filter(topic -> !reader.partitions(topic + SUFFIX).isEmpty()).toList();
      if (!restored.contains(changelogs.get(0))) {
        throw new IllegalStateException("Topic " + changelogs.get(0) + SUFFIX + " does not exist: there is no backup to restore.");
      }
      long recordedByEventify5 = 0;
      for (String topic : restored) {
        if (!reader.partitions(topic).equals(reader.partitions(topic + SUFFIX))) {
          throw new IllegalStateException("Topic " + topic + " no longer has the partitions of its backup.");
        }
        if (topic.equals(changelogs.get(0))) {
          recordedByEventify5 += recordedByEventify5(reader, topic);
        }
      }
      if (recordedByEventify5 > 0) {
        throw new IllegalStateException("Eventify 5 recorded " + recordedByEventify5 + " events of its own since the migration."
            + " A restore would lose them, so it is refused.");
      }
      try (ChangelogWriter writer = new ChangelogWriter(clientConfig, applicationId)) {
        for (String topic : restored) {
          for (int number : reader.partitions(topic)) {
            TopicPartition copy = new TopicPartition(topic + SUFFIX, number);
            long copyEnd = reader.end(copy);
            Map<String, Long> copyOffsets = reader.liveOffsets(copy, copyEnd);
            TopicPartition partition = new TopicPartition(topic, number);
            for (String key : reader.liveOffsets(partition, reader.end(partition)).keySet()) {
              if (!copyOffsets.containsKey(key)) {
                writer.send(topic, number, key, null);
                writer.changeWritten();
              }
            }
            reader.forEachLive(copy, copyEnd, copyOffsets, (key, value) -> {
              writer.send(topic, number, key, value);
              writer.changeWritten();
            });
          }
        }
        writer.commit();
      }
      for (String topic : restored) {
        done.add(compare(reader, topic + SUFFIX, topic, "Restored"));
      }
    }
    return done;
  }

  /** The events under a key the backup does not have that the migration did not write: their id is not an Eventify 4 key. */
  private long recordedByEventify5(ChangelogReader reader, String topic) {
    long count = 0;
    for (int number : reader.partitions(topic)) {
      TopicPartition copy = new TopicPartition(topic + SUFFIX, number);
      Set<String> copyKeys = reader.liveOffsets(copy, reader.end(copy)).keySet();
      TopicPartition partition = new TopicPartition(topic, number);
      long end = reader.end(partition);
      Map<String, Long> extra = new HashMap<>(reader.liveOffsets(partition, end));
      extra.keySet().removeAll(copyKeys);
      long[] found = {0};
      reader.forEachLive(partition, end, extra, (key, value) -> {
        if (Keys.v4Event(idOf(value)) == null) {
          found[0]++;
        }
      });
      count += found[0];
    }
    return count;
  }

  private String idOf(byte[] value) {
    try {
      return objectMapper.readTree(value).path("id").asText("");
    } catch (Exception e) {
      return "";
    }
  }

  /** Both topics must hold the same records, each in the same partition. */
  private static String compare(ChangelogReader reader, String from, String to, String what) {
    Content expected = content(reader, from);
    Content actual = content(reader, to);
    if (!expected.equals(actual)) {
      throw new IllegalStateException(what + " " + from + " to " + to + ", but they differ: " + expected.records()
          + " records against " + actual.records() + ". Do not go on.");
    }
    return what + " " + from + " to " + to + ": " + actual.records() + " records, compared.";
  }

  /** The number of records that exist and a sum of their hashes, which does not depend on their order. */
  private static Content content(ChangelogReader reader, String topic) {
    long[] records = {0};
    long[] sum = {0};
    for (int number : reader.partitions(topic)) {
      TopicPartition partition = new TopicPartition(topic, number);
      long end = reader.end(partition);
      reader.forEachLive(partition, end, reader.liveOffsets(partition, end), (key, value) -> {
        records[0]++;
        sum[0] += hash(number, key, value);
      });
    }
    return new Content(records[0], sum[0]);
  }

  private static long hash(int partition, String key, byte[] value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(ByteBuffer.allocate(4).putInt(partition).array());
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
      digest.update(ByteBuffer.allocate(4).putInt(keyBytes.length).array());
      digest.update(keyBytes);
      digest.update(value);
      return ByteBuffer.wrap(digest.digest()).getLong();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /** A topic with the partitions, replication and topic settings of the original. */
  private static void createLike(Admin admin, String original, String copy) {
    try {
      TopicDescription description = admin.describeTopics(List.of(original)).allTopicNames().get().get(original);
      ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, original);
      Config config = admin.describeConfigs(List.of(resource)).all().get().get(resource);
      Map<String, String> settings = new HashMap<>();
      for (ConfigEntry entry : config.entries()) {
        if (entry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
          settings.put(entry.name(), entry.value());
        }
      }
      short replication = (short) description.partitions().get(0).replicas().size();
      admin.createTopics(List.of(new NewTopic(copy, description.partitions().size(), replication).configs(settings))).all().get();
    } catch (ExecutionException e) {
      throw new IllegalStateException("Cannot create " + copy + ": " + e.getCause().getMessage(), e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while creating " + copy + ".", e);
    }
  }

  private record Content(long records, long sum) {
  }
}
