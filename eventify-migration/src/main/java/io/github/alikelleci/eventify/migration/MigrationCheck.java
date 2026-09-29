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
 * Phase 1: reads both changelogs and reports what the migration would do, and every reason it cannot. It only reads,
 * so it can run while the Eventify 4 application is running; that is the dry run.
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
    CheckReport report = new CheckReport(aggregateType, applicationId + "-event-store-changelog",
        applicationId + "-snapshot-store-changelog", dropSnapshots);
    if (aggregateType.isBlank() || aggregateType.indexOf(Keys.NUL) >= 0) {
      report.conflict("The aggregate type must be the @AggregateRoot name of the Eventify 5 application, not \"" + aggregateType + "\".");
    }
    try (ChangelogReader reader = new ChangelogReader(consumerConfig)) {
      checkEvents(reader, report);
      checkSnapshots(reader, report);
    }
    report.duration = Duration.between(start, Instant.now());
    return report;
  }

  private void checkEvents(ChangelogReader reader, CheckReport report) {
    var partitions = reader.partitions(report.eventTopic);
    report.eventPartitions = partitions.size();
    if (partitions.isEmpty()) {
      report.conflict("Topic " + report.eventTopic + " does not exist. Is the application id right?");
      return;
    }
    Map<String, Integer> partitionOfAggregate = new HashMap<>();
    for (int number : partitions) {
      TopicPartition partition = new TopicPartition(report.eventTopic, number);
      long end = reader.end(partition);
      Map<String, Long> liveOffsets = reader.liveOffsets(partition, end);
      Map<String, Long> eventsOfAggregate = new HashMap<>();
      TreeSet<String> v4Keys = new TreeSet<>();

      reader.forEachLive(partition, end, liveOffsets, (key, value) -> {
        if (Keys.isV5(key)) {
          report.conflict("Already an Eventify 5 key: " + Keys.printable(key) + ". A partly migrated store is not handled yet.");
          return;
        }
        Keys.V4EventKey v4Key = Keys.v4Event(key);
        if (v4Key == null) {
          report.conflict("Not an Eventify 4 event key: " + Keys.printable(key));
          return;
        }
        if (!checkEvent(v4Key, value, report)) {
          return;
        }
        Integer other = partitionOfAggregate.putIfAbsent(v4Key.aggregateId(), number);
        if (other != null && other != number) {
          report.conflict("Aggregate " + v4Key.aggregateId() + " has events in partitions " + other + " and " + number + ".");
        }
        eventsOfAggregate.merge(v4Key.aggregateId(), 1L, Long::sum);
        v4Keys.add(key);
      });

      eventsOfAggregate.forEach((aggregateId, events) -> {
        report.events += events;
        report.aggregates++;
        if (events > report.largestAggregateEvents) {
          report.largestAggregate = aggregateId;
          report.largestAggregateEvents = events;
        }
      });
      findSharedRanges(eventsOfAggregate.keySet(), v4Keys, report);
    }
  }

  /** Whether the stored event can be read as an Eventify 4 event of the aggregate its key names. */
  private boolean checkEvent(Keys.V4EventKey key, byte[] value, CheckReport report) {
    JsonNode event;
    try {
      event = objectMapper.readTree(value);
    } catch (Exception e) {
      report.conflict("Event " + key.key() + " is not JSON: " + e.getMessage());
      return false;
    }
    if (!key.aggregateId().equals(event.path("aggregateId").asText(null))) {
      report.conflict("Event " + key.key() + " has aggregateId " + event.path("aggregateId") + ", not the one in its key.");
      return false;
    }
    String payloadClass = event.path("payload").path("@class").asText(null);
    if (payloadClass == null) {
      report.conflict("Event " + key.key() + " has no payload with an @class.");
      return false;
    }
    report.eventClasses.merge(payloadClass, 1L, Long::sum);
    return true;
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
  private void checkSnapshots(ChangelogReader reader, CheckReport report) {
    var partitions = reader.partitions(report.snapshotTopic);
    report.snapshotPartitions = partitions.size();
    for (int number : partitions) {
      TopicPartition partition = new TopicPartition(report.snapshotTopic, number);
      for (String key : reader.liveOffsets(partition, reader.end(partition)).keySet()) {
        if (Keys.isV5(key)) {
          report.conflict("Already an Eventify 5 snapshot key: " + Keys.printable(key) + ". A partly migrated store is not handled yet.");
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
}
