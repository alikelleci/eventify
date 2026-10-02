package io.github.alikelleci.eventify.console.client;

import io.github.alikelleci.eventify.console.client.ConsoleViews.Result;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.plugin.PluginContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Which node answers for an aggregate: the one that owns it, as Kafka Streams tells every node. Another node answers
 * {@code NOT_OWNER} with the owner's name, and the console asks that node instead.
 */
@Slf4j
class Ownership {

  private final PluginContext eventify;
  private final HostInfo thisHost;

  Ownership(PluginContext eventify) {
    this.eventify = eventify;
    this.thisHost = NodeIdentity.hostInfo(eventify);
  }

  /**
   * Returns {@code null} when this instance can answer for the aggregate, or the reason it can't: Kafka Streams isn't
   * running, or another instance owns the aggregate. The console then asks that instance instead.
   */
  <T> Result<T> check(String aggregateId) {
    KafkaStreams streams = eventify.getKafkaStreams();

    if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
      log.debug("Kafka Streams is not running");
      return Result.unavailable("Kafka Streams is not running");
    }

    KeyQueryMetadata metadata = eventify.getAggregateMetadata(aggregateId);
    if (metadata == null || metadata.activeHost().equals(HostInfo.unavailable())) {
      log.warn("Metadata unavailable for aggregate {}", aggregateId);
      return Result.unavailable("Metadata unavailable");
    }

    HostInfo activeHost = metadata.activeHost();
    if (activeHost.equals(thisHost)) {
      return null;
    }

    return Result.notOwner(NodeIdentity.nodeId(activeHost));
  }

  /** Nothing found; unless this instance stopped owning the aggregate during the query, and simply no longer has it. */
  <T> Result<T> notFound(String aggregateId) {
    if (!isLocallyAuthoritative(aggregateId)) {
      log.debug("Ownership/availability changed while querying aggregate {}; returning 503", aggregateId);
      return Result.unavailable("Ownership changed during query");
    }
    return Result.notFound();
  }

  private boolean isLocallyAuthoritative(String aggregateId) {
    KafkaStreams streams = eventify.getKafkaStreams();
    if (streams.state() != KafkaStreams.State.RUNNING) {
      return false;
    }
    KeyQueryMetadata metadata = eventify.getAggregateMetadata(aggregateId);
    return metadata != null && thisHost.equals(metadata.activeHost());
  }
}
