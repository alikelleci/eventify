package io.github.alikelleci.eventify.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Reads both changelogs and reports what the migration would do, and every reason it cannot. It only reads, so it can
 * run while the Eventify 4 application is running (the dry run), and it runs again after the write (the verify).
 * <p>
 * A store may be partly migrated by an interrupted run. Such an event is recognised by its id, which is still its
 * Eventify 4 key, and it must have the sequence it would get now; the events that are left are numbered around it.
 */
final class MigrationCheck {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Properties consumerConfig;
  private final String aggregateType;
  private final boolean dropSnapshots;

  MigrationCheck(Properties consumerConfig, String aggregateType, boolean dropSnapshots) {
    this.consumerConfig = consumerConfig;
    this.aggregateType = aggregateType;
    this.dropSnapshots = dropSnapshots;
  }

  CheckReport run(String applicationId) {
    Instant start = Instant.now();
    CheckReport report = new CheckReport(aggregateType, applicationId, dropSnapshots);
    if (aggregateType.isBlank() || aggregateType.indexOf(Keys.NUL) >= 0) {
      report.conflict("The aggregate type must be the @AggregateRoot name of the Eventify 5 application, not \"" + aggregateType + "\".");
    }
    try (ChangelogReader reader = new ChangelogReader(consumerConfig)) {
      var partitions = reader.partitions(report.eventTopic);
      report.eventPartitions = partitions.size();
      if (partitions.isEmpty()) {
        report.conflict("Topic " + report.eventTopic + " does not exist. Is the application id right?");
      }
      Map<String, Integer> partitionOfAggregate = new HashMap<>();
      for (int partition : partitions) {
        scanEvents(reader, new TopicPartition(report.eventTopic, partition), report, partitionOfAggregate);
      }
      scanSnapshots(reader, report);
    }
    report.duration = Duration.between(start, Instant.now());
    return report;
  }

  /** Reads one partition of the event store and gives each Eventify 4 event that is left the sequence it gets. */
  PartitionScan scanEvents(ChangelogReader reader, TopicPartition partition, CheckReport report, Map<String, Integer> partitionOfAggregate) {
    long end = reader.end(partition);
    Map<String, Long> liveOffsets = reader.liveOffsets(partition, end);
    Map<String, TreeMap<String, Slot>> slotsOfAggregate = new HashMap<>();
    TreeSet<String> v4Keys = new TreeSet<>();

    reader.forEachLive(partition, end, liveOffsets, (key, value) -> {
      Keys.V4EventKey oldKey = Keys.isV5(key) ? migrated(key, value, report) : unmigrated(key, value, report);
      if (oldKey == null) {
        return;
      }
      Integer other = partitionOfAggregate.putIfAbsent(oldKey.aggregateId(), partition.partition());
      if (other != null && other != partition.partition()) {
        report.conflict("Aggregate " + oldKey.aggregateId() + " has events in partitions " + other + " and " + partition.partition() + ".");
      }
      Slot slot = slotsOfAggregate.computeIfAbsent(oldKey.aggregateId(), id -> new TreeMap<>())
          .computeIfAbsent(oldKey.ulid(), ulid -> new Slot());
      if (Keys.isV5(key)) {
        if (slot.migratedTo != null) {
          report.conflict("Event " + oldKey.key() + " was migrated twice, to #" + slot.migratedTo + " and to " + Keys.printable(key) + ".");
        }
        slot.migratedTo = Keys.v5Event(key).sequence();
      } else {
        slot.unmigrated = true;
      }
      v4Keys.add(oldKey.key());
    });

    Map<String, Long> sequenceOfOldKey = new HashMap<>();
    slotsOfAggregate.forEach((aggregateId, slots) -> {
      long sequence = 0;
      for (Map.Entry<String, Slot> entry : slots.entrySet()) {
        sequence++;
        String oldKey = aggregateId + "@" + entry.getKey();
        Slot slot = entry.getValue();
        if (slot.migratedTo != null && slot.migratedTo != sequence) {
          report.conflict("Event " + oldKey + " was migrated to #" + slot.migratedTo + ", but its place is #" + sequence + ".");
        }
        if (slot.unmigrated) {
          sequenceOfOldKey.put(oldKey, sequence);
        } else {
          report.migratedEvents++;
        }
      }
      report.events += sequence;
      report.aggregates++;
      if (sequence > report.largestAggregateEvents) {
        report.largestAggregate = aggregateId;
        report.largestAggregateEvents = sequence;
      }
    });
    findSharedRanges(slotsOfAggregate.keySet(), v4Keys, report);
    return new PartitionScan(end, liveOffsets, sequenceOfOldKey);
  }

  /** The Eventify 4 key of an event this migration has not written yet, or null when it cannot be migrated. */
  private Keys.V4EventKey unmigrated(String key, byte[] value, CheckReport report) {
    Keys.V4EventKey oldKey = Keys.v4Event(key);
    if (oldKey == null) {
      report.conflict("Not an Eventify 4 event key: " + Keys.printable(key));
      return null;
    }
    JsonNode event = readEvent(key, value, report);
    if (event == null) {
      return null;
    }
    if (!oldKey.aggregateId().equals(event.path("aggregateId").asText(null))) {
      report.conflict("Event " + key + " has aggregateId " + event.path("aggregateId") + ", not the one in its key.");
      return null;
    }
    return oldKey;
  }

  /** The Eventify 4 key of an event this migration wrote, or null when Eventify 5 wrote it or it is not consistent. */
  private Keys.V4EventKey migrated(String key, byte[] value, CheckReport report) {
    Keys.V5EventKey newKey = Keys.v5Event(key);
    if (newKey == null) {
      report.conflict("Not an Eventify 5 event key: " + Keys.printable(key));
      return null;
    }
    JsonNode event = readEvent(Keys.printable(key), value, report);
    if (event == null) {
      return null;
    }
    Keys.V4EventKey oldKey = Keys.v4Event(event.path("id").asText(""));
    if (oldKey == null) {
      report.conflict("Event " + Keys.printable(key) + " was written by Eventify 5 itself, not by this migration: the new version already ran on this store.");
      return null;
    }
    if (!newKey.aggregateType().equals(aggregateType) || !newKey.aggregateId().equals(oldKey.aggregateId())
        || !aggregateType.equals(event.path("aggregateType").asText(null))
        || !oldKey.aggregateId().equals(event.path("aggregateId").asText(null))
        || event.path("sequence").asLong(-1) != newKey.sequence()) {
      report.conflict("Event " + Keys.printable(key) + " (formerly " + oldKey.key() + ") does not match its key.");
      return null;
    }
    return oldKey;
  }

  private JsonNode readEvent(String key, byte[] value, CheckReport report) {
    JsonNode event;
    try {
      event = objectMapper.readTree(value);
    } catch (Exception e) {
      report.conflict("Event " + key + " is not JSON: " + e.getMessage());
      return null;
    }
    String payloadClass = event.path("payload").path("@class").asText(null);
    if (payloadClass == null) {
      report.conflict("Event " + key + " has no payload with an @class.");
      return null;
    }
    report.eventClasses.merge(payloadClass, 1L, Long::sum);
    return event;
  }

  /** Eventify 4 read {@code id@} up to {@code id@~}, which also holds {@code id@...@ULID} of another aggregate. */
  private static void findSharedRanges(Iterable<String> aggregateIds, TreeSet<String> v4Keys, CheckReport report) {
    Map<String, Long> included = new TreeMap<>();
    for (String aggregateId : aggregateIds) {
      included.clear();
      for (String key : v4Keys.subSet(aggregateId + "@", true, aggregateId + "@~", true)) {
        String other = Keys.v4Event(key).aggregateId();
        if (!other.equals(aggregateId)) {
          included.merge(other, 1L, Long::sum);
        }
      }
      included.forEach((other, events) -> report.sharedRanges.add(new CheckReport.SharedRange(aggregateId, other, events)));
    }
  }

  /**
   * Eventify 4 counted only the events it had a handler for, and counted events of a shared range, so its snapshot
   * version is not an Eventify 5 sequence and cannot be migrated. Snapshots that are only a cache can be deleted; when
   * events before a snapshot were deleted (deleteEvents), the snapshot is the only record of them.
   */
  private void scanSnapshots(ChangelogReader reader, CheckReport report) {
    var partitions = reader.partitions(report.snapshotTopic);
    report.snapshotPartitions = partitions.size();
    for (int number : partitions) {
      TopicPartition partition = new TopicPartition(report.snapshotTopic, number);
      for (String key : reader.liveOffsets(partition, reader.end(partition)).keySet()) {
        if (Keys.isV5(key)) {
          report.conflict("Snapshot " + Keys.printable(key) + " was written by Eventify 5: the new version already ran on this store.");
        } else {
          report.v4Snapshots++;
        }
      }
    }
    if (report.v4Snapshots > 0 && !dropSnapshots) {
      report.conflict(report.v4Snapshots + " Eventify 4 snapshots cannot be migrated. If they are only a cache (no"
          + " @EnableSnapshotting(deleteEvents = true) was ever used), run again with --drop-snapshots to delete them.");
    }
  }

  /** One Eventify 4 event: still under its old key, already under its new key, or both after an interrupted write. */
  private static final class Slot {
    boolean unmigrated;
    Long migratedTo;
  }

  /** What the write needs of one partition: where the scan ended and the sequence of each event still to migrate. */
  record PartitionScan(long end, Map<String, Long> liveOffsets, Map<String, Long> sequenceOfOldKey) {
  }
}
