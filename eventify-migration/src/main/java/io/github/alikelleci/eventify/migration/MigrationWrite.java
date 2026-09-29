package io.github.alikelleci.eventify.migration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Phase 2: writes each Eventify 4 event again under its Eventify 5 key, with its aggregate type and sequence added,
 * and a tombstone for its old key, to the same partition. An event and its tombstone are in one transaction, so a
 * crash leaves every event under exactly one of its keys, and a second run finishes what the first one started.
 */
final class MigrationWrite {

  /** Numbers exactly as stored: a decimal read as a double could come back different (10.50, 0.1000000000000000055). */
  private final ObjectMapper objectMapper = new ObjectMapper()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
  private final Properties clientConfig;
  private final MigrationCheck check;
  private final String aggregateType;

  MigrationWrite(Properties clientConfig, MigrationCheck check, String aggregateType) {
    this.clientConfig = clientConfig;
    this.check = check;
    this.aggregateType = aggregateType;
  }

  /** Returns the number of events written; refuses to write while the application still runs. */
  long run(CheckReport report) {
    long written = 0;
    try (ChangelogReader reader = new ChangelogReader(clientConfig); ChangelogWriter writer = new ChangelogWriter(clientConfig, report.applicationId)) {
      Map<String, Integer> partitionOfAggregate = new HashMap<>();
      for (int number : reader.partitions(report.eventTopic)) {
        TopicPartition partition = new TopicPartition(report.eventTopic, number);
        CheckReport partitionReport = new CheckReport(aggregateType, report.applicationId, report.dropSnapshots);
        MigrationCheck.PartitionScan scan = check.scanEvents(reader, partition, partitionReport, partitionOfAggregate);
        if (partitionReport.hasConflicts()) {
          writer.commit();
          throw new IllegalStateException("Partition " + number + " changed since the check: " + partitionReport.conflicts);
        }
        reader.forEachLive(partition, scan.end(), scan.liveOffsets(), (key, value) -> {
          Long sequence = scan.sequenceOfOldKey().get(key);
          if (sequence != null) {
            Keys.V4EventKey oldKey = Keys.v4Event(key);
            writer.send(report.eventTopic, number, Keys.v5Event(aggregateType, oldKey.aggregateId(), sequence), withSequence(value, sequence));
            writer.send(report.eventTopic, number, key, null);
            writer.changeWritten();
          }
        });
        written += scan.sequenceOfOldKey().size();
      }
      if (report.dropSnapshots) {
        for (int number : reader.partitions(report.snapshotTopic)) {
          TopicPartition partition = new TopicPartition(report.snapshotTopic, number);
          for (String key : reader.liveOffsets(partition, reader.end(partition)).keySet()) {
            if (!Keys.isV5(key)) {
              writer.send(report.snapshotTopic, number, key, null);
              writer.changeWritten();
            }
          }
        }
      }
      writer.commit();
    }
    return written;
  }

  /** The stored JSON as it is, plus what Eventify 5 requires of a stored event; its id stays its Eventify 4 key. */
  private byte[] withSequence(byte[] value, long sequence) {
    try {
      ObjectNode event = (ObjectNode) objectMapper.readTree(value);
      event.put("aggregateType", aggregateType);
      event.put("sequence", sequence);
      return objectMapper.writeValueAsBytes(event);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
