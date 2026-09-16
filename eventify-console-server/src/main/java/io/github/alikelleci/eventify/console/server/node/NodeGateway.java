package io.github.alikelleci.eventify.console.server.node;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.github.alikelleci.eventify.console.server.ConsoleProperties;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Sends a request to an instance of an application.
 *
 * <p>Queries about an aggregate can only be answered by the instance that owns it. The console doesn't know which one
 * that is, but every instance does (Kafka Streams tells them): the first instance asked either answers, or replies
 * with the owner's name. The request then goes to the owner, once. The owner is remembered for next time.
 */
@Slf4j
@Component
public class NodeGateway {

  private final NodeRegistry registry;
  private final JsonMapper jsonMapper;
  private final Duration requestTimeout;

  /** The instance that last answered for an aggregate: asked first next time, which usually saves the redirect. */
  private final Cache<OwnerKey, String> owners = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterAccess(Duration.ofMinutes(10))
      .build();

  private record OwnerKey(String applicationId, String aggregateId) {
  }

  public NodeGateway(NodeRegistry registry, JsonMapper jsonMapper, ConsoleProperties properties) {
    this.registry = registry;
    this.jsonMapper = jsonMapper;
    this.requestTimeout = properties.requestTimeout();
  }

  public Mono<Reply> send(String applicationId, Route route, String aggregateId, byte[] data) {
    if (!route.ownerRouted()) {
      ConnectedNode node = registry.nextNode(applicationId);
      return node == null ? Mono.just(notConnected(applicationId)) : request(node, route, data);
    }

    OwnerKey key = new OwnerKey(applicationId, aggregateId);
    ConnectedNode first = nodeOf(applicationId, owners.getIfPresent(key));
    if (first == null) {
      first = registry.nextNode(applicationId);
    }
    if (first == null) {
      return Mono.just(notConnected(applicationId));
    }

    ConnectedNode asked = first;
    return request(asked, route, data).flatMap(reply -> {
      if (reply.header().status() != ReplyHeader.Status.NOT_OWNER) {
        remember(key, asked, reply);
        return Mono.just(reply);
      }

      ConnectedNode owner = nodeOf(applicationId, reply.header().owner());
      if (owner == null || owner == asked) {
        owners.invalidate(key);
        return Mono.just(Reply.of(ReplyHeader.unavailable(
            "The instance that owns this aggregate is not connected. The application may be rebalancing: try again.")));
      }

      return request(owner, route, data).map(second -> {
        if (second.header().status() == ReplyHeader.Status.NOT_OWNER) {
          // The instances don't agree on the owner: partitions are moving right now.
          owners.invalidate(key);
          return Reply.of(ReplyHeader.unavailable("The application is rebalancing: try again."));
        }
        remember(key, owner, second);
        return second;
      });
    });
  }

  private void remember(OwnerKey key, ConnectedNode node, Reply reply) {
    ReplyHeader.Status status = reply.header().status();
    if (status == ReplyHeader.Status.OK || status == ReplyHeader.Status.NOT_FOUND) {
      owners.put(key, node.nodeId());
    }
  }

  /** The connected instance with this name, if it belongs to the application. */
  private ConnectedNode nodeOf(String applicationId, String nodeId) {
    ConnectedNode node = registry.find(nodeId);
    return node != null && node.applicationId().equals(applicationId) ? node : null;
  }

  /** Asks one instance directly, for requests that are about the instance itself (e.g. {@link Route#STATUS}). */
  public Mono<Reply> sendTo(ConnectedNode node, Route route, byte[] data) {
    return request(node, route, data);
  }

  private Mono<Reply> request(ConnectedNode node, Route route, byte[] data) {
    Payload request = DefaultPayload.create(data, route.name().getBytes(StandardCharsets.UTF_8));
    return node.rsocket().requestResponse(request)
        .timeout(requestTimeout)
        .map(this::toReply)
        .switchIfEmpty(Mono.fromSupplier(() -> Reply.of(ReplyHeader.unavailable("Empty answer from instance " + node.nodeId()))))
        .onErrorResume(e -> {
          log.warn("Request {} to instance {} of application {} failed: {}", route, node.nodeId(), node.applicationId(), e.toString());
          return Mono.just(Reply.of(ReplyHeader.unavailable("No answer from instance " + node.nodeId())));
        });
  }

  private Reply toReply(Payload payload) {
    try {
      ReplyHeader header = jsonMapper.readValue(payload.getMetadataUtf8(), ReplyHeader.class);
      return new Reply(header, toBytes(payload.getData()));
    } finally {
      payload.release();
    }
  }

  private static Reply notConnected(String applicationId) {
    return Reply.of(ReplyHeader.unavailable("No instance of application '" + applicationId + "' is connected"));
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }
}
