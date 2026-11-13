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
   * running, the owner is unknown, or another instance owns the aggregate. In the last case the console asks that one.
   */
  <T> Result<T> check(String aggregateId) {
    KafkaStreams streams = eventify.getKafkaStreams();

    if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
      log.debug("Cannot load aggregate, Kafka Streams is not running: aggregateId={}, streamsState={}", aggregateId, streams == null ? "not initialized" : streams.state());
      return Result.unavailable("Kafka Streams is not running.");
    }

    KeyQueryMetadata metadata = eventify.getAggregateMetadata(aggregateId);
    if (metadata == null || metadata.activeHost().equals(HostInfo.unavailable())) {
      log.warn("Cannot locate aggregate owner: aggregateId={}", aggregateId);
      return Result.unavailable("Metadata unavailable.");
    }

    HostInfo activeHost = metadata.activeHost();
    if (activeHost.equals(thisHost)) {
      return null;
    }

    return Result.notOwner(NodeIdentity.nodeId(activeHost));
  }

  /** Not found, or unavailable when this instance stopped owning the aggregate during the query. */
  <T> Result<T> notFound(String aggregateId) {
    if (!isLocallyAuthoritative(aggregateId)) {
      log.debug("Aggregate no longer available on this instance: aggregateId={}", aggregateId);
      return Result.unavailable("Ownership changed during query.");
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
