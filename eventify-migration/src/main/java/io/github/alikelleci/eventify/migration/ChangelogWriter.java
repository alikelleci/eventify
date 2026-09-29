package io.github.alikelleci.eventify.migration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Writes to the changelogs in transactions, and only while the application is stopped. It commits every
 * {@link #CHANGES_PER_TRANSACTION} changes, never within one: a change is what must be written together, such as an
 * event under its new key and the tombstone of its old key.
 */
final class ChangelogWriter implements AutoCloseable {

  /** Small enough to stay far below the transaction timeout. */
  private static final int CHANGES_PER_TRANSACTION = 5_000;

  private final KafkaProducer<String, byte[]> producer;
  private boolean open;
  private long changes;

  /** Refuses while the application still has running members. */
  ChangelogWriter(Properties clientConfig, String applicationId) {
    String running = runningMembers(clientConfig, applicationId);
    if (running != null) {
      throw new IllegalStateException(running);
    }
    Properties properties = new Properties();
    properties.putAll(clientConfig);
    // One fixed id: a second run for the same application fences the first instead of writing next to it.
    properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "eventify-migration-" + applicationId);
    this.producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
    producer.initTransactions();
  }

  void send(String topic, int partition, String key, byte[] value) {
    if (!open) {
      producer.beginTransaction();
      open = true;
    }
    producer.send(new ProducerRecord<>(topic, partition, key, value));
  }

  /** Marks the end of one change: what was sent since the previous one is committed together. */
  void changeWritten() {
    if (++changes % CHANGES_PER_TRANSACTION == 0) {
      commit();
    }
  }

  void commit() {
    if (open) {
      producer.commitTransaction();
      open = false;
    }
  }

  /** Closing without {@link #commit()} leaves the open transaction to be aborted. */
  @Override
  public void close() {
    producer.close();
  }

  /** Kafka Streams uses the application id as its consumer group: members mean the application still runs. */
  private static String runningMembers(Properties clientConfig, String applicationId) {
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
}
