package io.github.alikelleci.eventify.migration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Phase 2: writes each Eventify 4 event again under its Eventify 5 key, with its aggregate type and sequence added,
 * and a tombstone for its old key, to the same partition. An event and its tombstone are in one transaction, so a
 * crash leaves every event under exactly one of its keys, and a second run finishes what the first one started.
 */
final class MigrationWrite {

  /** Events per transaction: small enough to stay far below the transaction timeout. */
  private static final int EVENTS_PER_TRANSACTION = 5_000;

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

  /** Returns the number of events written; refuses to write while the application still has running members. */
  long run(CheckReport report) {
    String running = runningMembers(report.applicationId);
    if (running != null) {
      throw new IllegalStateException(running);
    }
    long written = 0;
    try (ChangelogReader reader = new ChangelogReader(clientConfig); KafkaProducer<String, byte[]> producer = producer(report.applicationId)) {
      producer.initTransactions();
      Transaction transaction = new Transaction(producer);
      Map<String, Integer> partitionOfAggregate = new HashMap<>();
      for (int number : reader.partitions(report.eventTopic)) {
        TopicPartition partition = new TopicPartition(report.eventTopic, number);
        CheckReport partitionReport = new CheckReport(aggregateType, report.applicationId, report.dropSnapshots);
        MigrationCheck.PartitionScan scan = check.scanEvents(reader, partition, partitionReport, partitionOfAggregate);
        if (partitionReport.hasConflicts()) {
          transaction.commit();
          throw new IllegalStateException("Partition " + number + " changed since the check: " + partitionReport.conflicts);
        }
        reader.forEachLive(partition, scan.end(), scan.liveOffsets(), (key, value) -> {
          Long sequence = scan.sequenceOfOldKey().get(key);
          if (sequence != null) {
            Keys.V4EventKey oldKey = Keys.v4Event(key);
            transaction.send(new ProducerRecord<>(report.eventTopic, number, Keys.v5Event(aggregateType, oldKey.aggregateId(), sequence),
                withSequence(value, sequence)));
            transaction.send(new ProducerRecord<>(report.eventTopic, number, key, null));
            transaction.eventWritten();
          }
        });
        written += scan.sequenceOfOldKey().size();
      }
      if (report.dropSnapshots) {
        for (int number : reader.partitions(report.snapshotTopic)) {
          TopicPartition partition = new TopicPartition(report.snapshotTopic, number);
          for (String key : reader.liveOffsets(partition, reader.end(partition)).keySet()) {
            if (!Keys.isV5(key)) {
              transaction.send(new ProducerRecord<>(report.snapshotTopic, number, key, null));
              transaction.eventWritten();
            }
          }
        }
      }
      transaction.commit();
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

  /** Kafka Streams uses the application id as its consumer group: members mean the application still runs. */
  private String runningMembers(String applicationId) {
    try (Admin admin = Admin.create(clientConfig)) {
      ConsumerGroupDescription group = admin.describeConsumerGroups(List.of(applicationId)).all().get().get(applicationId);
      if (group.members().isEmpty()) {
        return null;
      }
      return "Application " + applicationId + " is still running (" + group.members().size() + " members). Stop every instance first.";
    } catch (ExecutionException e) {
      if (e.getCause() instanceof GroupIdNotFoundException) {
        return null;
      }
      return "Cannot tell whether application " + applicationId + " is stopped: " + e.getCause().getMessage();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return "Interrupted while checking whether application " + applicationId + " is stopped.";
    }
  }

  private KafkaProducer<String, byte[]> producer(String applicationId) {
    Properties properties = new Properties();
    properties.putAll(clientConfig);
    // One fixed id: a second migration of the same application fences the first instead of writing next to it.
    properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "eventify-migration-" + applicationId);
    return new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
  }

  /** Commits every {@link #EVENTS_PER_TRANSACTION} events, never between an event and its tombstone. */
  private static final class Transaction {
    private final KafkaProducer<String, byte[]> producer;
    private boolean open;
    private int events;

    Transaction(KafkaProducer<String, byte[]> producer) {
      this.producer = producer;
    }

    void send(ProducerRecord<String, byte[]> record) {
      if (!open) {
        producer.beginTransaction();
        open = true;
      }
      producer.send(record);
    }

    void eventWritten() {
      if (++events % EVENTS_PER_TRANSACTION == 0) {
        commit();
      }
    }

    void commit() {
      if (open) {
        producer.commitTransaction();
        open = false;
      }
    }
  }
}
