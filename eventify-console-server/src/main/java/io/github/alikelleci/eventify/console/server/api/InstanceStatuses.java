package io.github.alikelleci.eventify.console.server.api;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.console.protocol.InstanceStatus;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.github.alikelleci.eventify.console.server.node.ConnectedNode;
import io.github.alikelleci.eventify.console.server.node.NodeGateway;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Optional;

/**
 * How the connected instances are doing: each one is asked for its own status. The UI asks for this every few seconds,
 * from every open page, so the answers are kept for a moment: pages asking at the same time share one question to each
 * instance.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InstanceStatuses {

  /** How long an answer is kept. Shorter than the UI asks, so every page still sees a fresh status each time. */
  private static final Duration KEEP = Duration.ofSeconds(2);

  /** How long to wait for an instance. The list of applications waits for this, so an instance that hangs is left out. */
  private static final Duration TIMEOUT = Duration.ofSeconds(2);

  private final NodeGateway gateway;
  private final JsonMapper jsonMapper;

  /** By node id; empty when the instance didn't answer. */
  private final AsyncCache<String, Optional<InstanceStatus>> cache = Caffeine.newBuilder()
      .expireAfterWrite(KEEP)
      .buildAsync();

  /**
   * The status of one instance, unless it was asked a moment ago; empty when it doesn't answer. Pages asking at the same
   * time wait for the same answer, so one that stops waiting (e.g. it refreshed) must not cancel it for the others.
   */
  public Mono<Optional<InstanceStatus>> of(ConnectedNode node) {
    return Mono.fromFuture(() -> cache.get(node.nodeId(), (key, executor) -> ask(node).toFuture()), true);
  }

  private Mono<Optional<InstanceStatus>> ask(ConnectedNode node) {
    return gateway.sendTo(node, Route.STATUS, new byte[0])
        .timeout(TIMEOUT, Mono.empty())
        .map(reply -> read(node, reply))
        .defaultIfEmpty(Optional.empty());
  }

  private Optional<InstanceStatus> read(ConnectedNode node, Reply reply) {
    if (reply.header().status() != ReplyHeader.Status.OK || reply.body().length == 0) {
      return Optional.empty();
    }
    try {
      return Optional.of(jsonMapper.readValue(reply.body(), InstanceStatus.class)).filter(status -> status.state() != null);
    } catch (Exception e) {
      log.warn("Could not read the status of instance {}", node.nodeId(), e);
      return Optional.empty();
    }
  }
}
